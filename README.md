# JWST API

A modern REST API for accessing James Webb Space Telescope observation data, powered by NASA's MAST archive.

## Features

- 🚀 Fast and modern REST API built with FastAPI
- 🔍 Search observations by target, instrument, filter, and date
- 📸 Access high-quality JWST images and metadata
- 🆓 Free and open source
- 📚 Automatic API documentation

## API Endpoints

- `GET /observations` - List all observations
- `GET /observations/{id}` - Get a specific observation
- `GET /observations/search` - Search observations
- `GET /observations/latest` - Get recent observations
- `GET /observations/random` - Get a random observation
- `GET /instruments` - List JWST instruments
- `GET /targets` - List observed targets

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