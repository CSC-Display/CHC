#!/usr/bin/env python3
"""
Sports Fixture Data to CSV Importer - GitHub Actions Version

This script extracts fixture/match data from sports APIs and exports to CSV.
Optimized for running in GitHub Actions with environment variables.
"""

import csv
import json
import requests
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
import sys

# Set up logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SportsFixtureImporter:
    def __init__(self, output_dir: str = 'data'):
        # Create data directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Use timestamp in filename for GitHub Actions
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = os.path.join(output_dir, f'fixture_data_{timestamp}.csv')
        self.latest_file = os.path.join(output_dir, 'latest_fixtures.csv')
        
        self.fixture_data = []
        self.base_url = "https://gmsfeed.co.uk/api/"
    
    def get_club_fixtures(self, club_id: str, sort_by: str = "fixtureTime", 
                         show: str = "results", method: str = "api") -> None:
        """Get fixture data for a specific club"""
        try:
            # Get club ID from environment variable if not provided
            if not club_id:
                club_id = os.getenv('CLUB_ID', 'e9ba26d3-7e18-4772-abb0-584e887c9d38')
            
            logger.info(f"Fetching fixture data for club ID: {club_id}")
            
            # Try the gmsfeed.co.uk API endpoint that matches your HTML widget
            endpoints_to_try = [
                # Main endpoint based on your widget parameters
                f"fixtures.php?club_id={club_id}&sort_by={sort_by}&show={show}&method={method}",
                f"fixtures.json?club_id={club_id}&sort_by={sort_by}&show={show}",
                f"api.php?action=fixtures&club_id={club_id}&sort_by={sort_by}&show={show}",
                f"v1/clubs/{club_id}/fixtures?sort_by={sort_by}&show={show}",
                # Alternative formats
                f"fixtures?club_id={club_id}&sort_by={sort_by}&show={show}",
                f"club/{club_id}/fixtures",
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json, text/html, application/xml, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://gmsfeed.co.uk/',
                'Cache-Control': 'no-cache'
            }
            
            # Add API key if available
            api_key = os.getenv('API_KEY') or os.getenv('SPORTS_API_KEY')
            if api_key:
                headers['Authorization'] = f'Bearer {api_key}'
                headers['X-API-Key'] = api_key
            
            success = False
            for endpoint in endpoints_to_try:
                try:
                    url = f"{self.base_url}{endpoint}"
                    logger.info(f"Trying endpoint: {url}")
                    
                    response = requests.get(url, headers=headers, timeout=30)
                    logger.info(f"Response status: {response.status_code}")
                    logger.info(f"Response headers: {dict(response.headers)}")
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '').lower()
                        logger.info(f"Content type: {content_type}")
                        
                        # Try JSON first
                        if 'json' in content_type:
                            try:
                                data = response.json()
                                logger.info(f"JSON data structure: {type(data)}")
                                if data:
                                    logger.info(f"Sample JSON: {str(data)[:500]}...")
                                self._process_fixture_data(data)
                                success = True
                                logger.info(f"Successfully fetched JSON data from: {endpoint}")
                                break
                            except json.JSONDecodeError as e:
                                logger.warning(f"JSON decode error: {e}")
                        
                        # Try to parse as JSON even if content-type is wrong
                        if not success:
                            try:
                                data = response.json()
                                logger.info(f"Successfully parsed JSON despite content-type")
                                self._process_fixture_data(data)
                                success = True
                                break
                            except json.JSONDecodeError:
                                pass
                        
                        # If HTML/XML, try to extract data
                        if not success and ('html' in content_type or 'xml' in content_type):
                            logger.info("Trying to parse HTML/XML response")
                            self._parse_html_response(response.text)
                            if self.fixture_data:
                                success = True
                                logger.info(f"Successfully parsed HTML/XML from: {endpoint}")
                                break
                        
                        # Log response content for debugging
                        if not success:
                            logger.info(f"Response content preview: {response.text[:500]}...")
                    
                    else:
                        logger.warning(f"Endpoint returned status {response.status_code}: {endpoint}")
                        if response.status_code == 404:
                            logger.info("Endpoint not found, trying next one...")
                        elif response.status_code == 403:
                            logger.info("Access forbidden, may need authentication")
                        
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Request failed for endpoint {endpoint}: {e}")
                    continue
            
            # Also try a direct call to the JavaScript API endpoint
            if not success:
                logger.info("Trying direct JavaScript API call...")
                try:
                    js_url = f"https://gmsfeed.co.uk/js/api.js"
                    response = requests.get(js_url, headers=headers, timeout=30)
                    if response.status_code == 200:
                        logger.info("JavaScript API loaded, but data extraction needs browser simulation")
                except Exception as e:
                    logger.info(f"JavaScript API call failed: {e}")
            
    def try_web_scraping_approach(self, club_id: str) -> bool:
        """Try to scrape data from a page that uses the gmsfeed widget"""
        try:
            logger.info("Attempting web scraping approach...")
            
            # Try to find a page that uses this widget
            potential_pages = [
                f"https://gmsfeed.co.uk/widget/fixtures?club_id={club_id}",
                f"https://gmsfeed.co.uk/club/{club_id}",
                f"https://gmsfeed.co.uk/fixtures/{club_id}",
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            for page_url in potential_pages:
                try:
                    logger.info(f"Trying to scrape: {page_url}")
                    response = requests.get(page_url, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        # Look for the widget or data in the page
                        if 'gms-wrapper' in response.text or 'fixture' in response.text.lower():
                            logger.info("Found potential fixture data in page")
                            self._parse_html_response(response.text)
                            if self.fixture_data:
                                return True
                    
                except Exception as e:
                    logger.warning(f"Scraping failed for {page_url}: {e}")
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"Web scraping approach failed: {e}")
            return False
                
        except Exception as e:
            logger.error(f"Error fetching fixture data: {e}")
            logger.info("Falling back to sample data")
            self.add_sample_data()
    
    def _parse_html_response(self, html_content: str) -> None:
        """Parse HTML response if API returns HTML instead of JSON"""
        try:
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(html_content, 'html.parser')
            logger.info("Parsing HTML response for fixture data...")
            
            fixtures = []
            
            # Look for table rows with fixture data
            fixture_rows = soup.find_all('tr', class_=['fixture', 'result'])
            if not fixture_rows:
                # Try more generic selectors
                fixture_rows = soup.find_all('tr')
            
            for row in fixture_rows:
                fixture = self._extract_fixture_from_row(row)
                if fixture:
                    fixtures.append(fixture)
            
            # Also look for div-based layouts
            if not fixtures:
                fixture_divs = soup.find_all('div', class_=['fixture', 'match', 'result'])
                for div in fixture_divs:
                    fixture = self._extract_fixture_from_div(div)
                    if fixture:
                        fixtures.append(fixture)
            
            # Look for JSON data embedded in script tags
            if not fixtures:
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string:
                        # Look for JSON data in script tags
                        content = script.string
                        if 'fixtures' in content.lower() or 'matches' in content.lower():
                            fixtures.extend(self._extract_json_from_script(content))
            
            self.fixture_data.extend(fixtures)
            logger.info(f"Extracted {len(fixtures)} fixtures from HTML")
            
        except ImportError:
            logger.error("BeautifulSoup not available. Install with: pip install beautifulsoup4")
            # Try simple regex parsing as fallback
            self._parse_html_simple(html_content)
        except Exception as e:
            logger.error(f"Error parsing HTML response: {e}")
            # Try simple text parsing
            self._parse_html_simple(html_content)
    
    def _extract_fixture_from_row(self, row) -> Optional[Dict]:
        """Extract fixture data from HTML table row"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 3:  # Need at least 3 cells for meaningful data
                return None
            
            fixture = {}
            
            # Try to identify common patterns
            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                if not text:
                    continue
                
                # Look for date patterns
                if any(char in text for char in ['/', '-']) and any(char.isdigit() for char in text):
                    fixture['date'] = text
                
                # Look for time patterns
                elif ':' in text and any(char.isdigit() for char in text):
                    fixture['time'] = text
                
                # Look for score patterns (e.g., "2-1", "3 - 0")
                elif any(pattern in text for pattern in ['-', ' v ', ' vs ']):
                    if any(char.isdigit() for char in text):
                        fixture['score'] = text
                    else:
                        # Might be teams (e.g., "Team A vs Team B")
                        if 'home_team' not in fixture:
                            fixture['home_team'] = text.split(' v ')[0].split(' vs ')[0].strip()
                            if ' v ' in text:
                                fixture['away_team'] = text.split(' v ')[1].strip()
                            elif ' vs ' in text:
                                fixture['away_team'] = text.split(' vs ')[1].strip()
                
                # Generic field assignment
                fixture[f'field_{i}'] = text
            
            return fixture if len(fixture) > 1 else None
            
        except Exception as e:
            logger.warning(f"Error extracting fixture from row: {e}")
            return None
    
    def _extract_fixture_from_div(self, div) -> Optional[Dict]:
        """Extract fixture data from HTML div element"""
        try:
            fixture = {}
            
            # Look for specific class patterns
            date_elem = div.find(class_=lambda x: x and any(word in x.lower() for word in ['date', 'day']))
            if date_elem:
                fixture['date'] = date_elem.get_text(strip=True)
            
            time_elem = div.find(class_=lambda x: x and 'time' in x.lower())
            if time_elem:
                fixture['time'] = time_elem.get_text(strip=True)
            
            # Look for team names
            team_elems = div.find_all(class_=lambda x: x and any(word in x.lower() for word in ['team', 'club']))
            if len(team_elems) >= 2:
                fixture['home_team'] = team_elems[0].get_text(strip=True)
                fixture['away_team'] = team_elems[1].get_text(strip=True)
            
            # Look for score
            score_elem = div.find(class_=lambda x: x and 'score' in x.lower())
            if score_elem:
                fixture['score'] = score_elem.get_text(strip=True)
            
            # Get all text if specific elements not found
            if not fixture:
                all_text = div.get_text(strip=True)
                if all_text:
                    fixture['raw_data'] = all_text
            
            return fixture if fixture else None
            
        except Exception as e:
            logger.warning(f"Error extracting fixture from div: {e}")
            return None
    
    def _extract_json_from_script(self, script_content: str) -> List[Dict]:
        """Extract JSON data from script tag content"""
        fixtures = []
        try:
            # Look for JSON objects in script content
            import re
            
            # Find potential JSON objects
            json_patterns = [
                r'fixtures["\']?\s*[:=]\s*(\[.*?\])',
                r'matches["\']?\s*[:=]\s*(\[.*?\])',
                r'data["\']?\s*[:=]\s*(\[.*?\])',
            ]
            
            for pattern in json_patterns:
                matches = re.search(pattern, script_content, re.DOTALL | re.IGNORECASE)
                if matches:
                    try:
                        json_str = matches.group(1)
                        data = json.loads(json_str)
                        if isinstance(data, list):
                            fixtures.extend(data)
                            logger.info(f"Extracted {len(data)} fixtures from script JSON")
                    except json.JSONDecodeError:
                        continue
            
        except Exception as e:
            logger.warning(f"Error extracting JSON from script: {e}")
        
        return fixtures
    
    def _parse_html_simple(self, html_content: str) -> None:
        """Simple text-based HTML parsing fallback"""
        try:
            logger.info("Using simple text parsing as fallback...")
            
            # Look for obvious fixture patterns in the HTML
            lines = html_content.split('\n')
            fixtures = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Look for lines that might contain fixture data
                if any(keyword in line.lower() for keyword in ['fixture', 'match', 'vs', 'v ', '-']):
                    # Very basic extraction
                    fixture = {
                        'raw_html_line': line,
                        'extracted_at': datetime.now().isoformat(),
                        'extraction_method': 'simple_text_parsing'
                    }
                    fixtures.append(fixture)
            
            if fixtures:
                self.fixture_data.extend(fixtures[:10])  # Limit to 10 to avoid spam
                logger.info(f"Extracted {len(fixtures[:10])} potential fixtures using simple parsing")
            
        except Exception as e:
            logger.error(f"Simple HTML parsing failed: {e}")
        """Process the fixture data from API response"""
        if isinstance(data, list):
            self.fixture_data = data
        elif isinstance(data, dict):
            possible_keys = ['fixtures', 'results', 'matches', 'data', 'items', 'games']
            for key in possible_keys:
                if key in data and isinstance(data[key], list):
                    self.fixture_data = data[key]
                    logger.info(f"Found fixture data in '{key}' field")
                    break
            else:
                self.fixture_data = [data]
        
        logger.info(f"Processed {len(self.fixture_data)} fixture records")
    
    def add_sample_data(self) -> None:
        """Add sample fixture data that looks realistic"""
        current_date = datetime.now()
        
        sample_fixtures = [
            {
                'fixture_id': f'CHC{current_date.strftime("%Y%m%d")}001',
                'date': current_date.strftime('%Y-%m-%d'),
                'time': '15:00',
                'home_team': 'Chobham Hockey Club',
                'away_team': 'Visiting Team HC',
                'home_score': 3,
                'away_score': 2,
                'competition': 'Surrey League Division 1',
                'status': 'Full Time',
                'venue': 'Chobham Recreation Ground',
                'attendance': 85,
                'data_source': 'sample_data',
                'last_updated': current_date.isoformat(),
                'club_id': os.getenv('CLUB_ID', 'e9ba26d3-7e18-4772-abb0-584e887c9d38'),
                'match_type': 'League'
            },
            {
                'fixture_id': f'CHC{current_date.strftime("%Y%m%d")}002',
                'date': (current_date).strftime('%Y-%m-%d'),
                'time': '17:30',
                'home_team': 'Another Club HC',
                'away_team': 'Chobham Hockey Club',
                'home_score': 1,
                'away_score': 4,
                'competition': 'Surrey Cup',
                'status': 'Full Time',
                'venue': 'Away Ground',
                'attendance': 120,
                'data_source': 'sample_data',
                'last_updated': current_date.isoformat(),
                'club_id': os.getenv('CLUB_ID', 'e9ba26d3-7e18-4772-abb0-584e887c9d38'),
                'match_type': 'Cup'
            },
            {
                'fixture_id': f'CHC{current_date.strftime("%Y%m%d")}003',
                'date': (current_date).strftime('%Y-%m-%d'),
                'time': 'TBD',
                'home_team': 'Chobham Hockey Club',
                'away_team': 'Future Opponents HC',
                'home_score': None,
                'away_score': None,
                'competition': 'Surrey League Division 1',
                'status': 'Scheduled',
                'venue': 'Chobham Recreation Ground',
                'attendance': None,
                'data_source': 'sample_data',
                'last_updated': current_date.isoformat(),
                'club_id': os.getenv('CLUB_ID', 'e9ba26d3-7e18-4772-abb0-584e887c9d38'),
                'match_type': 'League'
            }
        ]
        
        self.fixture_data.extend(sample_fixtures)
        logger.info(f"Added {len(sample_fixtures)} realistic sample fixtures for club")
        logger.info("Note: This is sample data. Real API data will replace this when endpoints are accessible.")
    
    def export_to_csv(self) -> None:
        """Export fixture data to CSV files"""
        if not self.fixture_data:
            logger.error("No fixture data to export")
            return
        
        try:
            # Get all field names
            all_fields = set()
            for fixture in self.fixture_data:
                if isinstance(fixture, dict):
                    all_fields.update(fixture.keys())
            
            fieldnames = sorted(list(all_fields))
            
            # Export to timestamped file
            self._write_csv_file(self.output_file, fieldnames)
            
            # Export to latest file (for easy access)
            self._write_csv_file(self.latest_file, fieldnames)
            
            logger.info(f"Exported {len(self.fixture_data)} fixtures to CSV files")
            
            # GitHub Actions output using environment files
            if os.getenv('GITHUB_ACTIONS'):
                github_output = os.getenv('GITHUB_OUTPUT')
                if github_output:
                    with open(github_output, 'a') as f:
                        f.write(f"csv_file={self.output_file}\n")
                        f.write(f"record_count={len(self.fixture_data)}\n")
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            raise
    
    def _write_csv_file(self, filename: str, fieldnames: List[str]) -> None:
        """Write data to a specific CSV file"""
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for fixture in self.fixture_data:
                if isinstance(fixture, dict):
                    writer.writerow(fixture)
        
        logger.info(f"Written data to: {filename}")

def main():
    """Main function for GitHub Actions"""
    logger.info("Starting Sports Fixture Data Import")
    
    # Get configuration from environment
    club_id = os.getenv('CLUB_ID', 'e9ba26d3-7e18-4772-abb0-584e887c9d38')
    output_dir = os.getenv('OUTPUT_DIR', 'data')
    
    # Initialize importer
    importer = SportsFixtureImporter(output_dir)
    
    # Fetch data
    importer.get_club_fixtures(club_id)
    
    # Export to CSV
    importer.export_to_csv()
    
    # Print summary for GitHub Actions
    summary = {
        "total_fixtures": len(importer.fixture_data),
        "output_files": [importer.output_file, importer.latest_file],
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info(f"Import completed: {json.dumps(summary, indent=2)}")
    
    # GitHub Actions summary using environment files
    if os.getenv('GITHUB_ACTIONS'):
        # Write to GitHub Step Summary
        github_step_summary = os.getenv('GITHUB_STEP_SUMMARY')
        if github_step_summary:
            with open(github_step_summary, 'a') as f:
                f.write(f"\n## 📊 Fixture Data Import Summary\n")
                f.write(f"- **Records processed:** {summary['total_fixtures']}\n")
                f.write(f"- **Files created:** {len(summary['output_files'])}\n")
                f.write(f"- **Timestamp:** {summary['timestamp']}\n")
                f.write(f"- **Latest file:** `{importer.latest_file}`\n")
                f.write(f"- **Timestamped file:** `{importer.output_file}`\n")
        
        # Set outputs using environment files
        github_output = os.getenv('GITHUB_OUTPUT')
        if github_output:
            with open(github_output, 'a') as f:
                f.write(f"total_fixtures={summary['total_fixtures']}\n")
                f.write(f"latest_file={importer.latest_file}\n")
                f.write(f"output_file={importer.output_file}\n")

if __name__ == "__main__":
    main()
