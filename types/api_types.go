package types

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
