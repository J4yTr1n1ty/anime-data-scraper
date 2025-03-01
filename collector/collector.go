package collector

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/J4yTr1n1ty/anime-data-scraper/api"
	"github.com/J4yTr1n1ty/anime-data-scraper/config"
	"github.com/J4yTr1n1ty/anime-data-scraper/exporter"
	"github.com/J4yTr1n1ty/anime-data-scraper/types"
)

type Collector struct {
	apiClient *api.Client
	exporter  *exporter.CSVExporter
	logger    *log.Logger
}

func NewCollector() *Collector {
	return &Collector{
		apiClient: api.NewClient(),
		exporter:  exporter.NewCSVExporter(),
		logger:    log.New(os.Stdout, "[COLLECTOR] ", log.LstdFlags),
	}
}

// CollectAnimeData is the main workflow function to collect and export anime data
func (collector *Collector) CollectAnimeData() error {
	// Initialize API client
	c := api.NewClient()

	// Initialize CSV exporter
	exporter := exporter.NewCSVExporter()

	// Initialize output directory
	if err := exporter.Initialize(); err != nil {
		return err
	}

	// 1. Get top anime list
	collector.logger.Printf("Fetching top %d anime...", config.MaxItemsToFetch)
	topAnime, err := c.GetTopAnime(config.MaxItemsToFetch)
	if err != nil {
		return fmt.Errorf("failed to fetch top anime: %w", err)
	}

	// 2. Export basic anime data
	if err := exporter.ExportAnimeBasicData(topAnime); err != nil {
		return fmt.Errorf("failed to export basic anime data: %w", err)
	}

	// 3. Export anime genres
	if err := exporter.ExportAnimeGenres(topAnime); err != nil {
		return fmt.Errorf("failed to export anime genres: %w", err)
	}

	// 4. Export anime studios
	if err := exporter.ExportAnimeStudios(topAnime); err != nil {
		return fmt.Errorf("failed to export anime studios: %w", err)
	}

	// 5. Collect detailed statistics for each anime
	collector.logger.Println("Fetching detailed statistics for each anime...")
	var enrichedAnime []types.AnimeData

	// Process a subset for statistics (can be resource intensive)
	limit := min(config.MaxItemsToFetch, 100) // Limit to avoid too many API calls
	for i, anime := range topAnime {
		if i >= limit {
			break
		}

		collector.logger.Printf("Fetching details for anime %d/%d: %s (ID: %d)",
			i+1, limit, anime.Title, anime.MalID)

		detailedAnime, err := c.GetAnimeDetails(anime.MalID)
		if err != nil {
			collector.logger.Printf("Warning: Failed to get details for anime ID %d: %v", anime.MalID, err)
			continue
		}

		enrichedAnime = append(enrichedAnime, *detailedAnime)

		// Add a small delay between requests
		time.Sleep(config.RateLimitDelay)
	}

	// 6. Export anime statistics
	if err := exporter.ExportAnimeStatistics(enrichedAnime); err != nil {
		return fmt.Errorf("failed to export anime statistics: %w", err)
	}

	// 7. Collect and export reviews for a subset of anime
	var allReviews []types.ReviewData
	reviewLimit := 50                         // Reviews per anime
	animeForReviews := min(20, len(topAnime)) // Limit number of anime to collect reviews for

	collector.logger.Printf("Collecting reviews for top %d anime...", animeForReviews)
	for i := 0; i < animeForReviews; i++ {
		anime := topAnime[i]
		collector.logger.Printf("Fetching reviews for anime %d/%d: %s (ID: %d)",
			i+1, animeForReviews, anime.Title, anime.MalID)

		reviews, err := c.GetAnimeReviews(anime.MalID, reviewLimit)
		if err != nil {
			collector.logger.Printf("Warning: Failed to get reviews for anime ID %d: %v", anime.MalID, err)
			continue
		}

		allReviews = append(allReviews, reviews...)
	}

	if err := exporter.ExportAnimeReviews(allReviews); err != nil {
		return fmt.Errorf("failed to export anime reviews: %w", err)
	}

	collector.logger.Println("Data collection complete. Files exported to:", config.OutputDirectory)
	return nil
}
