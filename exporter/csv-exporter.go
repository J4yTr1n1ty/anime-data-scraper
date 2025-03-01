package exporter

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/J4yTr1n1ty/anime-data-scraper/config"
	"github.com/J4yTr1n1ty/anime-data-scraper/types"
)

type CSVExporter struct {
	logger *log.Logger
}

func NewCSVExporter() *CSVExporter {
	return &CSVExporter{
		logger: log.New(os.Stdout, "[CSV] ", log.LstdFlags),
	}
}

func (c *CSVExporter) SetLogger(logger *log.Logger) {
	c.logger = logger
}

// Initialize creates the output directory if it doesn't exist
func (c *CSVExporter) Initialize() error {
	if err := os.MkdirAll(config.OutputDirectory, 0755); err != nil {
		c.logger.Printf("Failed to create output directory: %v", err)
		return err
	}
	return nil
}

// ExportAnimeBasicData exports basic anime information to CSV
func (c *CSVExporter) ExportAnimeBasicData(animeList []types.AnimeData) error {
	file, err := os.Create(config.AnimeBasicCSV)
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

	c.logger.Printf("Exported %d anime records to %s", len(animeList), config.AnimeBasicCSV)
	return nil
}

// ExportAnimeGenres exports anime genre relationships to CSV
func (c *CSVExporter) ExportAnimeGenres(animeList []types.AnimeData) error {
	file, err := os.Create(config.AnimeGenresCSV)
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

	c.logger.Printf("Exported %d anime-genre relationships to %s", totalGenres, config.AnimeGenresCSV)
	return nil
}

// ExportAnimeStudios exports anime studio relationships to CSV
func (c *CSVExporter) ExportAnimeStudios(animeList []types.AnimeData) error {
	file, err := os.Create(config.AnimeStudiosCSV)
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

	c.logger.Printf("Exported %d anime-studio relationships to %s", totalStudios, config.AnimeStudiosCSV)
	return nil
}

// ExportAnimeStatistics exports detailed anime statistics to CSV
func (c *CSVExporter) ExportAnimeStatistics(animeList []types.AnimeData) error {
	file, err := os.Create(config.AnimeStatisticsCSV)
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

	c.logger.Printf("Exported statistics for %d anime to %s", animeWithStats, config.AnimeStatisticsCSV)
	return nil
}

// ExportAnimeReviews exports anime reviews to CSV
func (c *CSVExporter) ExportAnimeReviews(reviews []types.ReviewData) error {
	file, err := os.Create(config.AnimeReviewsCSV)
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

	c.logger.Printf("Exported %d anime reviews to %s", len(reviews), config.AnimeReviewsCSV)
	return nil
}
