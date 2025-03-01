// Package main provides a client for collecting anime statistics from Jikan API
// for business intelligence and OLAP purposes.
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// API endpoints and configuration
const (
	JikanBaseURL     = "https://api.jikan.moe/v4"
	AnimeEndpoint    = "/anime"
	TopAnimeEndpoint = "/top/anime"
	ReviewsEndpoint  = "/reviews/anime"
	MaxRetries       = 3
	RetryDelay       = 1 * time.Second
	RateLimitDelay   = 1 * time.Second // Jikan API has rate limiting
	DefaultPageSize  = 25
	MaxItemsToFetch  = 1000 // Adjust based on needs
	OutputDirectory  = "anime_data"
)

// Paths for output CSV files
var (
	AnimeBasicCSV      = filepath.Join(OutputDirectory, "anime_basic.csv")
	AnimeGenresCSV     = filepath.Join(OutputDirectory, "anime_genres.csv")
	AnimeStudiosCSV    = filepath.Join(OutputDirectory, "anime_studios.csv")
	AnimeStatisticsCSV = filepath.Join(OutputDirectory, "anime_statistics.csv")
	AnimeReviewsCSV    = filepath.Join(OutputDirectory, "anime_reviews.csv")
)

// AnimeResponse represents the API response structure for anime endpoints
type AnimeResponse struct {
	Data       []AnimeData `json:"data"`
	Pagination Pagination  `json:"pagination"`
}

// SingleAnimeResponse represents the API response for a single anime
type SingleAnimeResponse struct {
	Data AnimeData `json:"data"`
}

// ReviewResponse represents the API response structure for review endpoints
type ReviewResponse struct {
	Data       []ReviewData `json:"data"`
	Pagination Pagination   `json:"pagination"`
}

// Pagination represents the pagination information from API responses
type Pagination struct {
	LastVisiblePage int  `json:"last_visible_page"`
	HasNextPage     bool `json:"has_next_page"`
	CurrentPage     int  `json:"current_page"`
	Items           struct {
		Count   int `json:"count"`
		Total   int `json:"total"`
		PerPage int `json:"per_page"`
	} `json:"items"`
}

// AnimeData represents the core anime data structure
type AnimeData struct {
	MalID         int    `json:"mal_id"`
	URL           string `json:"url"`
	Title         string `json:"title"`
	TitleEnglish  string `json:"title_english"`
	TitleJapanese string `json:"title_japanese"`
	Type          string `json:"type"`
	Source        string `json:"source"`
	Episodes      int    `json:"episodes"`
	Status        string `json:"status"`
	Airing        bool   `json:"airing"`
	Aired         struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"aired"`
	Duration   string  `json:"duration"`
	Rating     string  `json:"rating"`
	Score      float64 `json:"score"`
	ScoredBy   int     `json:"scored_by"`
	Rank       int     `json:"rank"`
	Popularity int     `json:"popularity"`
	Members    int     `json:"members"`
	Favorites  int     `json:"favorites"`
	Synopsis   string  `json:"synopsis"`
	Background string  `json:"background"`
	Season     string  `json:"season"`
	Year       int     `json:"year"`
	Genres     []struct {
		MalID int    `json:"mal_id"`
		Type  string `json:"type"`
		Name  string `json:"name"`
	} `json:"genres"`
	Studios []struct {
		MalID int    `json:"mal_id"`
		Type  string `json:"type"`
		Name  string `json:"name"`
	} `json:"studios"`
	Statistics *AnimeStatistics `json:"statistics,omitempty"`
}

// AnimeStatistics represents the detailed statistics for an anime
type AnimeStatistics struct {
	Watching    int `json:"watching"`
	Completed   int `json:"completed"`
	OnHold      int `json:"on_hold"`
	Dropped     int `json:"dropped"`
	PlanToWatch int `json:"plan_to_watch"`
	Total       int `json:"total"`
	Scores      map[string]struct {
		Votes      int     `json:"votes"`
		Percentage float64 `json:"percentage"`
	} `json:"scores"`
}

// ReviewData represents review information
type ReviewData struct {
	MalID      int      `json:"mal_id"`
	URL        string   `json:"url"`
	Type       string   `json:"type"`
	ReviewedAt string   `json:"date"`
	Review     string   `json:"review"`
	Score      int      `json:"score"`
	Tags       []string `json:"tags"`
	IsSpoiler  bool     `json:"is_spoiler"`
	User       struct {
		Username string `json:"username"`
		URL      string `json:"url"`
		Images   struct {
			JPG struct {
				ImageURL string `json:"image_url"`
			} `json:"jpg"`
		} `json:"images"`
	} `json:"user"`
	Anime struct {
		MalID int    `json:"mal_id"`
		URL   string `json:"url"`
		Title string `json:"title"`
	} `json:"anime"`
}

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

	for attempt := 1; attempt <= MaxRetries; attempt++ {
		c.logger.Printf("Fetching URL (attempt %d): %s", attempt, url)

		// Apply rate limiting
		if attempt > 1 {
			time.Sleep(RetryDelay)
		}

		resp, err = c.httpClient.Get(url)
		if err != nil {
			c.logger.Printf("Request error: %v. Retrying...", err)
			continue
		}

		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			c.logger.Printf("Rate limited. Waiting before retry...")
			time.Sleep(RateLimitDelay * 2) // Wait longer for rate limiting
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
		return nil, fmt.Errorf("failed after %d attempts: %w", MaxRetries, err)
	}

	// Rate limiting - always wait between requests to avoid hitting limits
	time.Sleep(RateLimitDelay)

	return body, nil
}

// GetTopAnime retrieves the top-rated anime
func (c *Client) GetTopAnime(limit int) ([]AnimeData, error) {
	var allAnime []AnimeData
	page := 1

	for len(allAnime) < limit {
		url := fmt.Sprintf("%s%s?page=%d", JikanBaseURL, TopAnimeEndpoint, page)

		body, err := c.fetchData(url)
		if err != nil {
			return nil, err
		}

		var response AnimeResponse
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
func (c *Client) GetAnimeDetails(malID int) (*AnimeData, error) {
	url := fmt.Sprintf("%s%s/%d/full", JikanBaseURL, AnimeEndpoint, malID)

	body, err := c.fetchData(url)
	if err != nil {
		return nil, err
	}

	var response SingleAnimeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal anime details: %w", err)
	}

	return &response.Data, nil
}

// GetAnimeReviews retrieves reviews for a specific anime ID
func (c *Client) GetAnimeReviews(malID int, limit int) ([]ReviewData, error) {
	var allReviews []ReviewData
	page := 1

	for len(allReviews) < limit {
		url := fmt.Sprintf("%s%s/%d/reviews?page=%d", JikanBaseURL, AnimeEndpoint, malID, page)

		body, err := c.fetchData(url)
		if err != nil {
			return nil, err
		}

		var response ReviewResponse
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

// ExportAnimeBasicData exports basic anime information to CSV
func (c *Client) ExportAnimeBasicData(animeList []AnimeData) error {
	file, err := os.Create(AnimeBasicCSV)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"mal_id", "title", "title_english", "title_japanese", "type",
		"source", "episodes", "status", "airing", "aired_from", "aired_to",
		"duration", "rating", "score", "scored_by", "rank", "popularity",
		"members", "favorites", "season", "year",
	}

	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows
	for _, anime := range animeList {
		row := []string{
			strconv.Itoa(anime.MalID),
			anime.Title,
			anime.TitleEnglish,
			anime.TitleJapanese,
			anime.Type,
			anime.Source,
			strconv.Itoa(anime.Episodes),
			anime.Status,
			strconv.FormatBool(anime.Airing),
			anime.Aired.From,
			anime.Aired.To,
			anime.Duration,
			anime.Rating,
			strconv.FormatFloat(anime.Score, 'f', 2, 64),
			strconv.Itoa(anime.ScoredBy),
			strconv.Itoa(anime.Rank),
			strconv.Itoa(anime.Popularity),
			strconv.Itoa(anime.Members),
			strconv.Itoa(anime.Favorites),
			anime.Season,
			strconv.Itoa(anime.Year),
		}

		if err := writer.Write(row); err != nil {
			return err
		}
	}

	c.logger.Printf("Exported %d anime records to %s", len(animeList), AnimeBasicCSV)
	return nil
}

// ExportAnimeGenres exports anime genre relationships to CSV
func (c *Client) ExportAnimeGenres(animeList []AnimeData) error {
	file, err := os.Create(AnimeGenresCSV)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"anime_id", "genre_id", "genre_name"}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows
	var totalGenres int
	for _, anime := range animeList {
		for _, genre := range anime.Genres {
			row := []string{
				strconv.Itoa(anime.MalID),
				strconv.Itoa(genre.MalID),
				genre.Name,
			}

			if err := writer.Write(row); err != nil {
				return err
			}
			totalGenres++
		}
	}

	c.logger.Printf("Exported %d anime-genre relationships to %s", totalGenres, AnimeGenresCSV)
	return nil
}

// ExportAnimeStudios exports anime studio relationships to CSV
func (c *Client) ExportAnimeStudios(animeList []AnimeData) error {
	file, err := os.Create(AnimeStudiosCSV)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"anime_id", "studio_id", "studio_name"}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows
	var totalStudios int
	for _, anime := range animeList {
		for _, studio := range anime.Studios {
			row := []string{
				strconv.Itoa(anime.MalID),
				strconv.Itoa(studio.MalID),
				studio.Name,
			}

			if err := writer.Write(row); err != nil {
				return err
			}
			totalStudios++
		}
	}

	c.logger.Printf("Exported %d anime-studio relationships to %s", totalStudios, AnimeStudiosCSV)
	return nil
}

// ExportAnimeStatistics exports detailed anime statistics to CSV
func (c *Client) ExportAnimeStatistics(animeList []AnimeData) error {
	file, err := os.Create(AnimeStatisticsCSV)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"anime_id", "watching", "completed", "on_hold", "dropped",
		"plan_to_watch", "total_stats",
		"score_1_votes", "score_1_percentage",
		"score_2_votes", "score_2_percentage",
		"score_3_votes", "score_3_percentage",
		"score_4_votes", "score_4_percentage",
		"score_5_votes", "score_5_percentage",
		"score_6_votes", "score_6_percentage",
		"score_7_votes", "score_7_percentage",
		"score_8_votes", "score_8_percentage",
		"score_9_votes", "score_9_percentage",
		"score_10_votes", "score_10_percentage",
	}

	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows
	var animeWithStats int
	for _, anime := range animeList {
		// Skip if no statistics available
		if anime.Statistics == nil {
			continue
		}

		// Prepare score data
		scoreVotes := make([]string, 10)
		scorePercentages := make([]string, 10)

		// Initialize with empty values
		for i := 0; i < 10; i++ {
			scoreVotes[i] = "0"
			scorePercentages[i] = "0"
		}

		// Fill in available score data
		for scoreStr, scoreData := range anime.Statistics.Scores {
			scoreInt, err := strconv.Atoi(scoreStr)
			if err != nil {
				continue
			}

			if scoreInt >= 1 && scoreInt <= 10 {
				idx := scoreInt - 1
				scoreVotes[idx] = strconv.Itoa(scoreData.Votes)
				scorePercentages[idx] = strconv.FormatFloat(scoreData.Percentage, 'f', 2, 64)
			}
		}

		// Combine all fields
		row := []string{
			strconv.Itoa(anime.MalID),
			strconv.Itoa(anime.Statistics.Watching),
			strconv.Itoa(anime.Statistics.Completed),
			strconv.Itoa(anime.Statistics.OnHold),
			strconv.Itoa(anime.Statistics.Dropped),
			strconv.Itoa(anime.Statistics.PlanToWatch),
			strconv.Itoa(anime.Statistics.Total),
		}

		// Add score data
		for i := 0; i < 10; i++ {
			row = append(row, scoreVotes[i], scorePercentages[i])
		}

		if err := writer.Write(row); err != nil {
			return err
		}
		animeWithStats++
	}

	c.logger.Printf("Exported statistics for %d anime to %s", animeWithStats, AnimeStatisticsCSV)
	return nil
}

// ExportAnimeReviews exports anime reviews to CSV
func (c *Client) ExportAnimeReviews(reviews []ReviewData) error {
	file, err := os.Create(AnimeReviewsCSV)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"review_id", "anime_id", "anime_title",
		"reviewer_username", "score", "date",
		"is_spoiler", "tags", "review_text",
	}

	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows
	for _, review := range reviews {
		// Clean and truncate review text
		reviewText := strings.ReplaceAll(review.Review, "\n", " ")
		reviewText = strings.ReplaceAll(reviewText, "\r", " ")
		if len(reviewText) > 32000 {
			reviewText = reviewText[:32000] + "..."
		}

		row := []string{
			strconv.Itoa(review.MalID),
			strconv.Itoa(review.Anime.MalID),
			review.Anime.Title,
			review.User.Username,
			strconv.Itoa(review.Score),
			review.ReviewedAt,
			strconv.FormatBool(review.IsSpoiler),
			strings.Join(review.Tags, "|"),
			reviewText,
		}

		if err := writer.Write(row); err != nil {
			return err
		}
	}

	c.logger.Printf("Exported %d anime reviews to %s", len(reviews), AnimeReviewsCSV)
	return nil
}

// Initialize creates the output directory if it doesn't exist
func (c *Client) Initialize() error {
	if err := os.MkdirAll(OutputDirectory, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	return nil
}

// CollectAnimeData is the main workflow function to collect and export anime data
func (c *Client) CollectAnimeData() error {
	// Initialize output directory
	if err := c.Initialize(); err != nil {
		return err
	}

	// 1. Get top anime list
	c.logger.Printf("Fetching top %d anime...", MaxItemsToFetch)
	topAnime, err := c.GetTopAnime(MaxItemsToFetch)
	if err != nil {
		return fmt.Errorf("failed to fetch top anime: %w", err)
	}

	// 2. Export basic anime data
	if err := c.ExportAnimeBasicData(topAnime); err != nil {
		return fmt.Errorf("failed to export basic anime data: %w", err)
	}

	// 3. Export anime genres
	if err := c.ExportAnimeGenres(topAnime); err != nil {
		return fmt.Errorf("failed to export anime genres: %w", err)
	}

	// 4. Export anime studios
	if err := c.ExportAnimeStudios(topAnime); err != nil {
		return fmt.Errorf("failed to export anime studios: %w", err)
	}

	// 5. Collect detailed statistics for each anime
	c.logger.Println("Fetching detailed statistics for each anime...")
	var enrichedAnime []AnimeData

	// Process a subset for statistics (can be resource intensive)
	limit := min(MaxItemsToFetch, 100) // Limit to avoid too many API calls
	for i, anime := range topAnime {
		if i >= limit {
			break
		}

		c.logger.Printf("Fetching details for anime %d/%d: %s (ID: %d)",
			i+1, limit, anime.Title, anime.MalID)

		detailedAnime, err := c.GetAnimeDetails(anime.MalID)
		if err != nil {
			c.logger.Printf("Warning: Failed to get details for anime ID %d: %v", anime.MalID, err)
			continue
		}

		enrichedAnime = append(enrichedAnime, *detailedAnime)

		// Add a small delay between requests
		time.Sleep(RateLimitDelay)
	}

	// 6. Export anime statistics
	if err := c.ExportAnimeStatistics(enrichedAnime); err != nil {
		return fmt.Errorf("failed to export anime statistics: %w", err)
	}

	// 7. Collect and export reviews for a subset of anime
	var allReviews []ReviewData
	reviewLimit := 50                         // Reviews per anime
	animeForReviews := min(20, len(topAnime)) // Limit number of anime to collect reviews for

	c.logger.Printf("Collecting reviews for top %d anime...", animeForReviews)
	for i := 0; i < animeForReviews; i++ {
		anime := topAnime[i]
		c.logger.Printf("Fetching reviews for anime %d/%d: %s (ID: %d)",
			i+1, animeForReviews, anime.Title, anime.MalID)

		reviews, err := c.GetAnimeReviews(anime.MalID, reviewLimit)
		if err != nil {
			c.logger.Printf("Warning: Failed to get reviews for anime ID %d: %v", anime.MalID, err)
			continue
		}

		allReviews = append(allReviews, reviews...)
	}

	if err := c.ExportAnimeReviews(allReviews); err != nil {
		return fmt.Errorf("failed to export anime reviews: %w", err)
	}

	c.logger.Println("Data collection complete. Files exported to:", OutputDirectory)
	return nil
}

// Helper function for min (Go 1.18+ has built-in min function, but using this for compatibility)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	client := NewClient()

	start := time.Now()
	log.Println("Starting anime data collection for business intelligence...")

	if err := client.CollectAnimeData(); err != nil {
		log.Fatalf("Error: %v", err)
	}

	elapsed := time.Since(start)
	log.Printf("Data collection completed in %s", elapsed)
	log.Printf("CSV files are available in the '%s' directory", OutputDirectory)
}
