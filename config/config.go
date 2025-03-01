package config

import (
	"path/filepath"
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
