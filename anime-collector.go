package main

import (
	"log"
	"time"

	"github.com/J4yTr1n1ty/anime-data-scraper/collector"
	"github.com/J4yTr1n1ty/anime-data-scraper/config"
)

func main() {
	collector := collector.NewCollector()

	start := time.Now()
	log.Println("Starting anime data collection for business intelligence...")

	if err := collector.CollectAnimeData(); err != nil {
		log.Fatalf("Error: %v", err)
	}

	elapsed := time.Since(start)
	log.Printf("Data collection completed in %s", elapsed.Round(time.Second))
	log.Printf("CSV files are available in the '%s' directory", config.OutputDirectory)
}
