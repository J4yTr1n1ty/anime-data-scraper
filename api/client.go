package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/J4yTr1n1ty/anime-data-scraper/config"
	"github.com/J4yTr1n1ty/anime-data-scraper/types"
)

// Client manages the API client functionality
type Client struct {
	httpClient *http.Client
	logger     *log.Logger
}

// NewClient creates a new Jikan API client
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: log.New(os.Stdout, "[JIKAN] ", log.LstdFlags),
	}
}

// fetchData retrieves data from the Jikan API with retry logic
func (c *Client) fetchData(url string) ([]byte, error) {
	var (
		resp *http.Response
		err  error
		body []byte
	)

	for attempt := 1; attempt <= config.MaxRetries; attempt++ {
		c.logger.Printf("Fetching URL (attempt %d): %s", attempt, url)

		// Apply rate limiting
		if attempt > 1 {
			time.Sleep(config.RetryDelay)
		}

		resp, err = c.httpClient.Get(url)
		if err != nil {
			c.logger.Printf("Request error: %v. Retrying...", err)
			continue
		}

		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			c.logger.Printf("Rate limited. Waiting before retry...")
			time.Sleep(config.RateLimitDelay * 2) // Wait longer for rate limiting
			continue
		}

		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("API returned non-200 status code: %d", resp.StatusCode)
			c.logger.Printf("%v. Retrying...", err)
			continue
		}

		body, err = io.ReadAll(resp.Body)
		if err != nil {
			c.logger.Printf("Failed to read response body: %v. Retrying...", err)
			continue
		}

		// Success
		break
	}

	if err != nil {
		return nil, fmt.Errorf("failed after %d attempts: %w", config.MaxRetries, err)
	}

	// Rate limiting - always wait between requests to avoid hitting limits
	time.Sleep(config.RateLimitDelay)

	return body, nil
}

// GetTopAnime retrieves the top-rated anime
func (c *Client) GetTopAnime(limit int) ([]types.AnimeData, error) {
	var allAnime []types.AnimeData
	page := 1

	for len(allAnime) < limit {
		url := fmt.Sprintf("%s%s?page=%d", config.JikanBaseURL, config.TopAnimeEndpoint, page)

		body, err := c.fetchData(url)
		if err != nil {
			return nil, err
		}

		var response types.AnimeResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response: %w", err)
		}

		// No more data available
		if len(response.Data) == 0 || !response.Pagination.HasNextPage {
			break
		}

		// Add to results
		allAnime = append(allAnime, response.Data...)

		// Check if we've fetched enough
		if len(allAnime) >= limit {
			allAnime = allAnime[:limit]
			break
		}

		page++
	}

	c.logger.Printf("Retrieved %d top anime", len(allAnime))
	return allAnime, nil
}

// GetAnimeDetails retrieves detailed information for a specific anime ID
func (c *Client) GetAnimeDetails(malID int) (*types.AnimeData, error) {
	url := fmt.Sprintf("%s%s/%d/full", config.JikanBaseURL, config.AnimeEndpoint, malID)

	body, err := c.fetchData(url)
	if err != nil {
		return nil, err
	}

	var response types.SingleAnimeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal anime details: %w", err)
	}

	return &response.Data, nil
}

// GetAnimeReviews retrieves reviews for a specific anime ID
func (c *Client) GetAnimeReviews(malID int, limit int) ([]types.ReviewData, error) {
	var allReviews []types.ReviewData
	page := 1

	for len(allReviews) < limit {
		url := fmt.Sprintf("%s%s/%d/reviews?page=%d", config.JikanBaseURL, config.AnimeEndpoint, malID, page)

		body, err := c.fetchData(url)
		if err != nil {
			return nil, err
		}

		var response types.ReviewResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, fmt.Errorf("failed to unmarshal reviews: %w", err)
		}

		// No more data available
		if len(response.Data) == 0 || !response.Pagination.HasNextPage {
			break
		}

		// Add to results
		allReviews = append(allReviews, response.Data...)

		// Check if we've fetched enough
		if len(allReviews) >= limit {
			allReviews = allReviews[:limit]
			break
		}

		page++
	}

	c.logger.Printf("Retrieved %d reviews for anime ID %d", len(allReviews), malID)
	return allReviews, nil
}
