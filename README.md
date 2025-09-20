# Sports Fixture Data Importer

Automatically fetches and updates sports fixture data using GitHub Actions.

## Features

- 🔄 Automated daily data updates
- 📊 CSV export of fixture data
- 🏆 Supports multiple sports APIs
- 📱 Works with gmsfeed.co.uk API
- 🤖 GitHub Actions automation

## Setup

1. Fork this repository
2. Add any required API keys as GitHub Secrets:
   - Go to Settings > Secrets and variables > Actions
   - Add `SPORTS_API_KEY` if needed
3. The workflow will run automatically daily at 6 AM UTC

## Manual Run

To trigger a manual update:
1. Go to the Actions tab
2. Select "Update Sports Fixture Data"
3. Click "Run workflow"

## Data Output

The script generates CSV files with fixture data including:
- Match dates and times
- Team names
- Scores (for completed matches)
- Competition information
- Venue details

## Configuration

Edit `sports_fixture_importer.py` to:
- Change the club ID
- Modify API endpoints
- Adjust data fields
- Change output format

## Club ID

Current club ID: `e9ba26d3-7e18-4772-abb0-584e887c9d38`

To find your club ID:
1. Visit the gmsfeed.co.uk widget for your team
2. Look for the `data-club_id` attribute in the HTML

## License

MIT License
