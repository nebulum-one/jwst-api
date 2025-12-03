# JWST API

A modern REST API for accessing James Webb Space Telescope observation data, powered by NASA's MAST archive.

## Features

- 🚀 Fast and modern REST API built with FastAPI
- 🔍 Search observations by target, instrument, filter, and date
- 📸 Access high-quality JWST images and metadata
- 🆓 Free and open source
- 📚 Automatic API documentation

## API Endpoints

### Basic Endpoints
- `GET /` - API information and endpoint list
- `GET /health` - Health check with database status
- `GET /observations` - List observations with filters
- `GET /observations/{obs_id}` - Get specific observation
- `GET /observations/latest` - Most recent observations
- `GET /observations/random` - Random observation

### Search Endpoints
- `GET /observations/search` - Advanced search with multiple filters
- `GET /observations/search/coordinates` - Cone search by RA/Dec coordinates
- `GET /observations/search/date` - Search by date range

### Discovery Endpoints
- `GET /instruments` - List all instruments with counts
- `GET /filters` - List all filters with counts
- `GET /targets` - List observed targets with counts
- `GET /proposals` - List all proposals with details
- `GET /proposals/{proposal_id}` - Get all observations for a proposal
- `GET /statistics` - Comprehensive statistics

### Example Queries

**Search by coordinates (cone search):**
```
/observations/search/coordinates?ra=202.5&dec=47.3&radius=0.5
```

**Search by date range:**
```
/observations/search/date?start_date=2024-01-01&end_date=2024-12-31
```

**Search last 30 days:**
```
/observations/search/date?days_ago=30
```

**Advanced search:**
```
/observations/search?q=galaxy&instrument=NIRCAM&filter=F200W
```

**Filter by proposal:**
```
/observations?proposal_id=5816
```

## Tech Stack

- **Backend**: Python + FastAPI
- **Database**: PostgreSQL
- **Data Source**: NASA MAST Archive (via astroquery)
- **Hosting**: Railway

## Development Setup

### Prerequisites

- Python 3.10 or higher
- PostgreSQL (or use Railway's database)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/jwst-api.git
cd jwst-api
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file with:
```
DATABASE_URL=postgresql://user:password@localhost/jwst
```

4. Run the API:
```bash
uvicorn src.api.main:app --reload
```

5. Visit `http://localhost:8000/docs` for interactive API documentation

## Data Ingestion

The API fetches data from NASA's MAST archive using astroquery. To manually trigger a data fetch:

```bash
python src/jobs/fetch_jwst_data.py
```

## Deployment

This API is designed to be deployed on Railway:

1. Push your code to GitHub
2. Connect your Railway account to the repository
3. Add a PostgreSQL database service
4. Deploy!

Railway will automatically detect the configuration and deploy your API.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - feel free to use this project however you'd like!

## Acknowledgments

- NASA and the JWST team for the incredible data
- MAST Archive for providing programmatic access
- Railway for easy hosting

## Contact

Questions or suggestions? Open an issue on GitHub!