#!/usr/bin/env python3
"""
Anime Statistics Scraper

This script fetches anime statistics from MyAnimeList for OLAP and business intelligence purposes.
It collects data about popular anime including ratings, genres, studios, release dates, viewership metrics,
watch time statistics, and user reviews data.
Results are exported to CSV files for data warehouse ingestion.

Dependencies:
    - requests
    - beautifulsoup4
    - pandas
    - tqdm
    - lxml
"""

import requests
import pandas as pd
import time
import random
import json
import os
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Optional, Any, Union

# Configure constants
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
]
BASE_URL = "https://myanimelist.net"
OUTPUT_DIR = "anime_data"

class AnimeStatisticsScraper:
    """Scraper class for fetching anime statistics from MyAnimeList."""
    
    def __init__(self, rate_limit_delay: Tuple[float, float] = (1.0, 3.0)) -> None:
        """
        Initialize the scraper with rate limiting configuration.
        
        Args:
            rate_limit_delay: Tuple of (min_delay, max_delay) in seconds between requests
        """
        self.session = requests.Session()
        self.min_delay, self.max_delay = rate_limit_delay
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
    def _get_random_user_agent(self) -> str:
        """Return a random user agent from the predefined list."""
        return random.choice(USER_AGENTS)
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[BeautifulSoup]:
        """
        Make a rate-limited HTTP request.
        
        Args:
            url: Target URL
            params: Optional query parameters
            
        Returns:
            BeautifulSoup object or None if request failed
        """
        headers = {
            "User-Agent": self._get_random_user_agent(),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": BASE_URL
        }
        
        try:
            # Implement rate limiting
            delay = random.uniform(self.min_delay, self.max_delay)
            time.sleep(delay)
            
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            
            return BeautifulSoup(response.text, "lxml")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e} - URL: {url}")
            return None
            
    def get_top_anime(self, limit: int = 100) -> pd.DataFrame:
        """
        Fetch the top anime list.
        
        Args:
            limit: Maximum number of anime to fetch
            
        Returns:
            DataFrame containing top anime with basic information
        """
        page_size = 50  # Number of anime per page
        pages_to_fetch = (limit + page_size - 1) // page_size
        
        all_anime = []
        
        for page in tqdm(range(1, pages_to_fetch + 1), desc="Fetching top anime pages"):
            url = f"{BASE_URL}/topanime.php"
            params = {"limit": (page - 1) * page_size}

            soup = self._make_request(url, params)
            if not soup:
                continue
                
            # Extract anime entries
            anime_entries = soup.select("tr.ranking-list")
            
            for entry in anime_entries:
                if len(all_anime) >= limit:
                    break
                    
                rank_elem = entry.select_one("span.top-anime-rank-text")
                title_elem = entry.select_one("td.title")
                score_elem = entry.select_one("span.score-label")
                info_elem = entry.select_one("div.information")
                
                if not all([rank_elem, title_elem, score_elem, info_elem]):
                    continue

                print("Title: ", title_elem)
                
                # Extract anime ID from URL with robust error handling
                anime_id = None
                try:
                    url_path = title_elem.get("href", "")
                    if url_path:
                        url_parts = url_path.strip("/").split("/")
                        if len(url_parts) >= 2 and url_parts[-2].isdigit():
                            anime_id = int(url_parts[-2])
                except (ValueError, IndexError, AttributeError) as e:
                    print(f"Error extracting anime ID: {e} - URL: {title_elem.get('href', '')}")
                
                # Extract information text and parse
                info_text = info_elem.text.strip()
                anime_type = ""
                episodes = None
                aired = ""
                
                type_match = info_text.split("\n")[0].strip() if "\n" in info_text else info_text
                if "(" in type_match and ")" in type_match:
                    anime_type = type_match.split("(")[0].strip()
                    episodes_text = type_match.split("(")[1].split(")")[0].strip()
                    try:
                        if episodes_text.endswith(" eps"):
                            episodes = int(episodes_text.replace(" eps", ""))
                    except ValueError:
                        episodes = None
                
                members_text = info_elem.select_one("span.members")
                members = None
                if members_text:
                    try:
                        members = int(members_text.text.strip().replace(",", "").replace("members", "").strip())
                    except ValueError:
                        members = None
                
                # Only add entries with valid anime_id
                if anime_id is not None:
                    anime_info = {
                        "id": anime_id,
                        "rank": int(rank_elem.text.strip()) if rank_elem.text.strip().isdigit() else None,
                        "title": title_elem.text.strip(),
                        "url": title_elem.get("href"),
                        "score": float(score_elem.text.strip()) if score_elem.text.strip() else None,
                        "type": anime_type,
                        "episodes": episodes,
                        "members": members
                    }
                    all_anime.append(anime_info)
            
            if len(all_anime) >= limit:
                break
        
        # Ensure we have data before returning the DataFrame
        if not all_anime:
            print("Warning: No anime data was collected. Check the website structure or rate limits.")
            # Return empty DataFrame with expected columns to prevent errors
            return pd.DataFrame(columns=["id", "rank", "title", "url", "score", "type", "episodes", "members"])
        
        return pd.DataFrame(all_anime)
    
    def _parse_airing_dates(self, aired_text: str) -> Dict[str, Any]:
        """
        Parse airing date information from a string.
        
        Args:
            aired_text: String containing airing date information
            
        Returns:
            Dictionary with start_date, end_date, and airing_status
        """
        result = {
            "start_date": None,
            "end_date": None,
            "airing_status": "Unknown"
        }
        
        if not aired_text or aired_text == "Not available":
            return result
            
        # Check if currently airing
        if "to ?" in aired_text:
            result["airing_status"] = "Currently Airing"
        elif " to " in aired_text:
            result["airing_status"] = "Finished Airing"
        else:
            # Single date or format not recognized
            result["airing_status"] = "Aired"
            
        # Try to extract dates
        date_format = "%b %d, %Y"
        alternative_format = "%Y"
        
        try:
            if " to " in aired_text:
                start_str, end_str = aired_text.split(" to ")
                
                # Parse start date
                try:
                    result["start_date"] = datetime.strptime(start_str.strip(), date_format).date().isoformat()
                except ValueError:
                    # Try year-only format
                    if re.match(r'^\d{4}$', start_str.strip()):
                        result["start_date"] = datetime.strptime(start_str.strip(), alternative_format).date().isoformat()
                
                # Parse end date if not "?"
                if end_str.strip() != "?":
                    try:
                        result["end_date"] = datetime.strptime(end_str.strip(), date_format).date().isoformat()
                    except ValueError:
                        # Try year-only format
                        if re.match(r'^\d{4}$', end_str.strip()):
                            result["end_date"] = datetime.strptime(end_str.strip(), alternative_format).date().isoformat()
            else:
                # Single date
                try:
                    result["start_date"] = datetime.strptime(aired_text.strip(), date_format).date().isoformat()
                    result["end_date"] = result["start_date"]  # Same date for one-time airings
                except ValueError:
                    # Try year-only format
                    if re.match(r'^\d{4}$', aired_text.strip()):
                        result["start_date"] = datetime.strptime(aired_text.strip(), alternative_format).date().isoformat()
                        result["end_date"] = result["start_date"]
        except Exception as e:
            print(f"Error parsing airing dates '{aired_text}': {e}")
            
        return result
    
    def _calculate_total_runtime(self, episodes: Optional[int], minutes_per_episode: Optional[int]) -> Optional[int]:
        """
        Calculate total runtime in minutes for a series.
        
        Args:
            episodes: Number of episodes
            minutes_per_episode: Minutes per episode
            
        Returns:
            Total runtime in minutes or None if data is incomplete
        """
        if episodes is None or minutes_per_episode is None:
            return None
            
        return episodes * minutes_per_episode
    
    def get_anime_details(self, anime_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed information for a specific anime.
        
        Args:
            anime_id: Unique identifier for the anime
            
        Returns:
            Dictionary containing detailed anime information
        """
        url = f"{BASE_URL}/anime/{anime_id}"
        soup = self._make_request(url)
        
        if not soup:
            return None
            
        # Extract basic information
        title = soup.select_one("h1.title-name")
        title_text = title.text.strip() if title else ""
        
        # Extract various details
        details = {}
        
        # Extract from left info panel
        info_panel = soup.select_one("div.leftside")
        if info_panel:
            info_blocks = info_panel.select("div.spaceit_pad")
            for block in info_blocks:
                text = block.text.strip()
                if ":" in text:
                    key, value = text.split(":", 1)
                    details[key.strip().lower().replace(" ", "_")] = value.strip()
        
        # Extract score details
        score_elem = soup.select_one("div.score-label")
        score = None
        if score_elem:
            try:
                score = float(score_elem.text.strip())
            except ValueError:
                score = None
        
        # Extract statistics
        stats = {}
        stats_panel = soup.select_one("div.stats-block")
        if stats_panel:
            for item in stats_panel.select("div.spaceit_pad"):
                text = item.text.strip()
                if ":" in text:
                    key, value = text.split(":", 1)
                    stats[key.strip().lower().replace(" ", "_")] = value.strip()
        
        # Extract genres
        genres = []
        genre_elems = soup.select("span.genre a")
        for genre in genre_elems:
            genres.append(genre.text.strip())
        
        # Extract studio
        studios = []
        studio_elems = soup.select('span:-soup-contains("Studios:") ~ a')
        for studio in studio_elems:
            studios.append(studio.text.strip())
        
        # Extract synopsis
        synopsis_elem = soup.select_one("p[itemprop='description']")
        synopsis = synopsis_elem.text.strip() if synopsis_elem else ""
        
        # Parse airing dates
        aired_text = details.get("aired", "")
        airing_info = self._parse_airing_dates(aired_text)
        
        # Extract broadcast information (day and time)
        broadcast_info = {
            "day": None,
            "time": None
        }
        broadcast_text = details.get("broadcast", "")
        if broadcast_text and broadcast_text != "Unknown":
            # Format like "Tuesdays at 01:05 (JST)"
            day_match = re.search(r'([A-Za-z]+day)', broadcast_text)
            time_match = re.search(r'(\d{1,2}:\d{2})', broadcast_text)
            
            if day_match:
                broadcast_info["day"] = day_match.group(1)
            if time_match:
                broadcast_info["time"] = time_match.group(1)
        
        # Extract sample reviews
        reviews = self._get_sample_reviews(anime_id)
        
        # Combine all information
        anime_details = {
            "id": anime_id,
            "title": title_text,
            "score": score,
            "genres": genres,
            "studios": studios,
            "synopsis": synopsis,
            "details": details,
            "stats": stats,
            "url": url,
            "airing_info": airing_info,
            "broadcast_info": broadcast_info,
            "reviews": reviews,
            "scraped_at": datetime.now().isoformat()
        }
        
        return anime_details
    
    def _get_sample_reviews(self, anime_id: int, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Fetch sample reviews for an anime.
        
        Args:
            anime_id: Unique identifier for the anime
            limit: Maximum number of reviews to fetch
            
        Returns:
            List of review dictionaries
        """
        url = f"{BASE_URL}/anime/{anime_id}/reviews"
        soup = self._make_request(url)
        
        if not soup:
            return []
            
        reviews = []
        review_elements = soup.select("div.review-element")
        
        for i, review_elem in enumerate(review_elements):
            if i >= limit:
                break
                
            # Extract reviewer info
            reviewer_elem = review_elem.select_one("div.username a")
            reviewer = reviewer_elem.text.strip() if reviewer_elem else "Anonymous"
            
            # Extract review date
            date_elem = review_elem.select_one("div.update_at")
            review_date = None
            if date_elem:
                date_text = date_elem.text.strip()
                date_match = re.search(r'(\w+ \d+, \d{4})', date_text)
                if date_match:
                    try:
                        review_date = datetime.strptime(date_match.group(1), "%b %d, %Y").date().isoformat()
                    except ValueError:
                        review_date = None
            
            # Extract reviewer score
            score_elem = review_elem.select_one("div.rating span")
            score = None
            if score_elem:
                try:
                    score = int(score_elem.text.strip())
                except ValueError:
                    score = None
            
            # Extract review content (truncated)
            content_elem = review_elem.select_one("div.text")
            content = ""
            if content_elem:
                content = content_elem.text.strip()
                # Truncate long reviews
                if len(content) > 500:
                    content = content[:497] + "..."
            
            # Extract helpful count
            helpful_elem = review_elem.select_one("div.helpful_yes span")
            helpful_count = 0
            if helpful_elem:
                try:
                    helpful_count = int(helpful_elem.text.strip())
                except ValueError:
                    helpful_count = 0
            
            review_info = {
                "reviewer": reviewer,
                "date": review_date,
                "score": score,
                "content": content,
                "helpful_count": helpful_count
            }
            
            reviews.append(review_info)
        
        return reviews
    
    def batch_get_anime_details(self, anime_ids: List[int], max_workers: int = 5) -> List[Dict[str, Any]]:
        """
        Fetch details for multiple anime in parallel.
        
        Args:
            anime_ids: List of anime IDs to fetch
            max_workers: Maximum number of parallel workers
            
        Returns:
            List of anime details dictionaries
        """
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.get_anime_details, anime_id): anime_id for anime_id in anime_ids}
            
            for future in tqdm(futures, desc="Fetching anime details"):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    anime_id = futures[future]
                    print(f"Error fetching details for anime ID {anime_id}: {e}")
        
        return results
    
    def extract_dimensional_data(self, anime_details: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """
        Transform raw anime details into dimensional tables for OLAP.
        
        Args:
            anime_details: List of anime detail dictionaries
            
        Returns:
            Dictionary of dimensional DataFrames
        """
        # Anime dimension (facts)
        anime_facts = []
        for anime in anime_details:
            # Extract numerical values from stats
            stats = anime.get("stats", {})
            members = stats.get("members", "0").replace(",", "")
            favorites = stats.get("favorites", "0").replace(",", "")
            
            try:
                members = int(members)
            except ValueError:
                members = 0
                
            try:
                favorites = int(favorites)
            except ValueError:
                favorites = 0
            
            # Extract details
            details = anime.get("details", {})
            premiered = details.get("premiered", "")
            season = ""
            year = None
            
            if premiered:
                parts = premiered.split()
                if len(parts) >= 2:
                    season = parts[0]
                    try:
                        year = int(parts[1])
                    except ValueError:
                        year = None
            
            # Parse status
            status = details.get("status", "")
            
            # Parse episode count
            episodes_str = details.get("episodes", "")
            episodes = None
            try:
                if episodes_str and episodes_str != "Unknown":
                    episodes = int(episodes_str)
            except ValueError:
                episodes = None
                
            # Parse duration
            duration = details.get("duration", "")
            minutes_per_episode = None
            
            if "min" in duration:
                try:
                    # Extract minutes part
                    min_part = duration.split("min")[0].strip()
                    # Handle ranges like "23 min. per ep."
                    if "-" in min_part:
                        min_part = min_part.split("-")[-1].strip()
                    minutes_per_episode = int(min_part)
                except ValueError:
                    minutes_per_episode = None
            
            # Calculate total runtime
            total_runtime = self._calculate_total_runtime(episodes, minutes_per_episode)
            
            # Extract airing information
            airing_info = anime.get("airing_info", {})
            start_date = airing_info.get("start_date")
            end_date = airing_info.get("end_date")
            airing_status = airing_info.get("airing_status", "Unknown")
            
            # Extract broadcast information
            broadcast_info = anime.get("broadcast_info", {})
            broadcast_day = broadcast_info.get("day")
            broadcast_time = broadcast_info.get("time")
            
            anime_fact = {
                "anime_id": anime["id"],
                "title": anime["title"],
                "score": anime["score"],
                "episodes": episodes,
                "status": status,
                "season": season,
                "year": year,
                "members": members,
                "favorites": favorites,
                "minutes_per_episode": minutes_per_episode,
                "total_runtime_minutes": total_runtime,
                "start_date": start_date,
                "end_date": end_date,
                "airing_status": airing_status,
                "broadcast_day": broadcast_day,
                "broadcast_time": broadcast_time,
                "url": anime["url"]
            }
            anime_facts.append(anime_fact)
        
        # Genre dimension
        genres = []
        for anime in anime_details:
            anime_id = anime["id"]
            for genre in anime.get("genres", []):
                genres.append({
                    "anime_id": anime_id,
                    "genre": genre
                })
        
        # Studio dimension
        studios = []
        for anime in anime_details:
            anime_id = anime["id"]
            for studio in anime.get("studios", []):
                studios.append({
                    "anime_id": anime_id,
                    "studio": studio
                })
        
        # Reviews dimension
        reviews = []
        for anime in anime_details:
            anime_id = anime["id"]
            for review in anime.get("reviews", []):
                reviews.append({
                    "anime_id": anime_id,
                    "reviewer": review.get("reviewer"),
                    "date": review.get("date"),
                    "score": review.get("score"),
                    "content": review.get("content"),
                    "helpful_count": review.get("helpful_count", 0)
                })
        
        # Create DataFrames
        return {
            "anime_facts": pd.DataFrame(anime_facts) if anime_facts else pd.DataFrame(),
            "anime_genres": pd.DataFrame(genres) if genres else pd.DataFrame(),
            "anime_studios": pd.DataFrame(studios) if studios else pd.DataFrame(),
            "anime_reviews": pd.DataFrame(reviews) if reviews else pd.DataFrame()
        }
    
    def run_full_extraction(self, top_limit: int = 500, details_limit: int = 300) -> Dict[str, pd.DataFrame]:
        """
        Run the complete data extraction pipeline.
        
        Args:
            top_limit: Number of top anime to fetch
            details_limit: Number of anime to fetch detailed information for
            
        Returns:
            Dictionary of dimensional DataFrames
        """
        print(f"Fetching top {top_limit} anime...")
        top_anime = self.get_top_anime(limit=top_limit)
        
        # Save initial data regardless of potential later errors
        if not top_anime.empty:
            top_anime.to_csv(f"{OUTPUT_DIR}/top_anime.csv", index=False)
            print(f"Saved basic information for {len(top_anime)} anime.")
        else:
            print("Warning: No top anime data was collected.")
            return {}
        
        # Safely extract anime IDs with proper error handling
        try:
            anime_ids = []
            if 'id' in top_anime.columns:
                # Filter out any None or NaN values and convert to integers
                anime_ids = top_anime["id"].dropna().astype(int).tolist()[:details_limit]
            
            if not anime_ids:
                print("Error: No valid anime IDs found in the collected data.")
                return {"top_anime": top_anime}
                
            print(f"Fetching details for {len(anime_ids)} anime...")
            anime_details = self.batch_get_anime_details(anime_ids)
            
            if not anime_details:
                print("Warning: No detailed anime data was collected.")
                return {"top_anime": top_anime}
            
            # Save raw details as JSON
            with open(f"{OUTPUT_DIR}/anime_details_raw.json", "w", encoding="utf-8") as f:
                json.dump(anime_details, f, ensure_ascii=False, indent=2)
            
            # Extract dimensional data
            print("Transforming data into dimensional model...")
            dimensional_data = self.extract_dimensional_data(anime_details)
            
            # Save dimensional tables
            for name, df in dimensional_data.items():
                if not df.empty:
                    df.to_csv(f"{OUTPUT_DIR}/{name}.csv", index=False)
                    print(f"Saved {name} with {len(df)} rows.")
                else:
                    print(f"Warning: {name} table is empty.")
            
            dimensional_data["top_anime"] = top_anime
            print(f"Data extraction complete. Files saved to {OUTPUT_DIR}/")
            return dimensional_data
            
        except Exception as e:
            print(f"Error during data processing: {e}")
            import traceback
            traceback.print_exc()
            return {"top_anime": top_anime}

def main():
    """Main execution function."""
    print("Anime Statistics Data Collection for OLAP")
    print("========================================")
    
    # Initialize scraper with conservative rate limiting
    scraper = AnimeStatisticsScraper(rate_limit_delay=(2.0, 4.0))
    
    # Run extraction process
    try:
        scraper.run_full_extraction(top_limit=55, details_limit=30)
    except KeyboardInterrupt:
        print("\nExecution interrupted by user. Partial data may have been saved.")
    except Exception as e:
        print(f"Error during extraction: {e}")
        import traceback
        traceback.print_exc()
    
    print("\nExecution complete.")

if __name__ == "__main__":
    main()
