# JWST API

A modern REST API for accessing James Webb Space Telescope observation data (images & spectra), powered by NASA's MAST archive.

## Features

- 🚀 Fast and modern REST API built with FastAPI
- 🔍 Search observations by target, instrument, filter, and date
- 📸 Access high-quality JWST images and metadata
- 📊 **NEW:** Access spectroscopic data with wavelength ranges, gratings, and spectral resolution
- 🎯 Separate endpoints for images and spectra
- 🆓 Free and open source
- 📚 Automatic API documentation

## API Endpoints

### Basic Endpoints
- `GET /` - API information and endpoint list
- `GET /health` - Health check with database status
- `GET /observations` - List all observations with filters
- `GET /observations/{obs_id}` - Get specific observation by ID
- `GET /observations/latest` - Most recent observations
- `GET /observations/random` - Random observation

### Data Product Endpoints
- `GET /observations/images` - **List imaging observations only** (excludes spectra)
- `GET /observations/spectra` - **List spectroscopic observations only** with spectrum-specific filters

### Search Endpoints
- `GET /observations/search` - Advanced search with multiple filters
- `GET /observations/search/coordinates` - Cone search by RA/Dec coordinates
- `GET /observations/search/date` - Search by date range

### Discovery Endpoints
- `GET /instruments` - List all instruments with observation counts
- `GET /filters` - List all filters with observation counts
- `GET /gratings` - **NEW:** List all gratings/dispersers used in spectroscopy
- `GET /targets` - List observed targets with observation counts
- `GET /proposals` - List all proposals with details
- `GET /proposals/{proposal_id}` - Get all observations for a specific proposal
- `GET /statistics` - Comprehensive statistics (includes data product breakdown)

## Example Queries

### Basic Queries

**Get all observations:**
```
/observations?limit=50
```

**Get a specific observation:**
```
/observations/jw02736-o001_t001_nircam_clear-f090w
```

**Get latest observations:**
```
/observations/latest?limit=20
```

**Random observation:**
```
/observations/random
```

### Image-Specific Queries

**Get only imaging data:**
```
/observations/images?instrument=NIRCAM&filter=F200W
```

**NIRCam images of galaxies:**
```
/observations/images?instrument=NIRCAM&target=galaxy
```

### Spectrum-Specific Queries

**Get all spectroscopic observations:**
```
/observations/spectra?limit=50
```

**NIRSpec observations with high spectral resolution:**
```
/observations/spectra?instrument=NIRSPEC&min_resolution=1000
```

**Spectra in a specific wavelength range:**
```
/observations/spectra?min_wavelength=2.0&max_wavelength=5.0
```

**Observations using a specific grating:**
```
/observations/spectra?grating=G395H
```

### Coordinate-Based Search

**Cone search (find observations near coordinates):**
```
/observations/search/coordinates?ra=202.5&dec=47.3&radius=0.5
```

Parameters:
- `ra` - Right Ascension in degrees (0-360)
- `dec` - Declination in degrees (-90 to 90)
- `radius` - Search radius in degrees

### Date-Based Search

**Search by date range:**
```
/observations/search/date?start_date=2024-01-01&end_date=2024-12-31
```

**Search last 30 days:**
```
/observations/search/date?days_ago=30
```

### Advanced Search

**Text search across target names and descriptions:**
```
/observations/search?q=galaxy&instrument=NIRCAM&filter=F200W
```

**Filter by calibration level:**
```
/observations/search?calib_level=3&instrument=MIRI
```

**Filter by target classification:**
```
/observations/search?target_classification=GALAXY
```

### Discovery Queries

**List all instruments:**
```
/instruments
```

**List all filters:**
```
/filters?limit=100
```

**List all spectroscopic gratings:**
```
/gratings
```

**Most observed targets:**
```
/targets?limit=50
```

**Browse proposals:**
```
/proposals?limit=100
```

**Get all observations for a specific proposal:**
```
/proposals/2736
```

### Statistics

**Get comprehensive database statistics:**
```
/statistics
```

Returns:
- Total observations count
- Images vs spectra breakdown
- Instrument statistics
- Top targets
- Top filters
- Top gratings (for spectra)
- Date range coverage
- Total exposure time

## Response Format

### Standard Observation Response

```json
{
  "id": 123,
  "obs_id": "jw02736-o001_t001_nircam_clear-f090w",
  "target_name": "NGC-4321",
  "coordinates": {
    "ra": 185.729,
    "dec": 15.822
  },
  "instrument": "NIRCAM",
  "filter": "F090W",
  "observation_date": "2024-03-15T10:30:00",
  "preview_url": "https://mast.stsci.edu/...",
  "fits_url": "https://mast.stsci.edu/...",
  "description": "JWST NIRCam imaging of NGC 4321",
  "proposal_id": "2736",
  "exposure_time": 1234.5,
  "dataproduct_type": "image",
  "calib_level": 3,
  "wavelength_region": "INFRARED",
  "pi_name": "John Smith",
  "target_classification": "GALAXY"
}
```

### Spectrum Response (Additional Fields)

For spectroscopic observations, additional fields are included:

```json
{
  "dataproduct_type": "spectrum",
  "spectrum_metadata": {
    "spectral_resolution": 2700,
    "wavelength_range": {
      "min": 0.97,
      "max": 5.27,
      "unit": "microns"
    },
    "grating": "G395H",
    "dispersion_axis": null,
    "slit_width": null
  }
}
```

## Tech Stack

- **Backend**: Python 3.10+ with FastAPI
- **Database**: PostgreSQL
- **Data Source**: NASA MAST Archive (via astroquery)
- **ORM**: SQLAlchemy
- **Hosting**: Railway (or any Python hosting platform)

## Development Setup

### Prerequisites

- Python 3.10 or higher
- PostgreSQL (or use Railway's database)
- pip (Python package manager)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/jwst-api.git
cd jwst-api
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:

Create a `.env` file:
```env
DATABASE_URL=postgresql://user:password@localhost/jwst
```

5. Initialize the database:
```bash
python -c "from src.db.database import init_db; init_db()"
```

6. Run the API:
```bash
uvicorn src.api.main:app --reload
```

7. Visit `http://localhost:8000/docs` for interactive API documentation

## Data Ingestion

The API fetches data from NASA's MAST archive using astroquery. It processes observations month-by-month and tracks progress automatically.

### Fetch Next Month

To fetch the next incomplete month:
```bash
python src/jobs/fetch_jwst_data.py
```

### Re-fetch a Specific Month

To re-fetch a specific month (useful for updating old data to include spectra):
```bash
python src/jobs/fetch_jwst_data.py 2024-10
```

This will:
1. Delete existing data for that month
2. Re-fetch with updated schema (including spectrum metadata)
3. Update progress tracking

### Check Progress

View detailed progress of your data backfill:
```bash
python src/jobs/show_progress.py
```

### Database Schema

The database includes these spectrum-specific fields:
- `dataproduct_type` - "image" or "spectrum"
- `spectral_resolution` - R = λ/Δλ
- `wavelength_min` - Minimum wavelength in microns
- `wavelength_max` - Maximum wavelength in microns
- `grating` - Grating/disperser used (e.g., G395H, PRISM)
- `dispersion_axis` - Spectral dispersion direction
- `slit_width` - Slit width in arcseconds

## Deployment

### Deploy on Railway

1. Push your code to GitHub
2. Create a new project on [Railway](https://railway.app)
3. Connect your GitHub repository
4. Add a PostgreSQL database service
5. Add environment variables:
   - `DATABASE_URL` (automatically set by Railway PostgreSQL)
6. Deploy!

Railway will automatically:
- Detect your Python app
- Install dependencies from `requirements.txt`
- Run your FastAPI application
- Provide a public URL

### Deploy on Other Platforms

This API can be deployed on any platform that supports Python:
- **Heroku**: Add `Procfile` with `web: uvicorn src.api.main:app --host 0.0.0.0 --port $PORT`
- **AWS**: Use Elastic Beanstalk or Lambda with API Gateway
- **DigitalOcean**: Use App Platform
- **Google Cloud**: Use Cloud Run

## API Rate Limiting

Currently, there are no rate limits. For production use, consider adding:
- Rate limiting middleware (e.g., `slowapi`)
- Caching layer (e.g., Redis)
- CDN for static assets

## Contributing

Contributions are welcome! Here's how to help:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add docstrings to new functions
- Update README for new features
- Test endpoints before submitting PR

## Future Enhancements

Potential features for future versions:
- [ ] Authentication and API keys
- [ ] Rate limiting
- [ ] Caching layer
- [ ] Advanced filtering (by wavelength, exposure time ranges)
- [ ] Download multiple FITS files as ZIP
- [ ] GraphQL endpoint
- [ ] WebSocket support for real-time updates
- [ ] Thumbnail generation service
- [ ] FITS file metadata extraction
- [ ] Cross-matching with astronomical catalogs

## License

MIT License - feel free to use this project however you'd like!

## Acknowledgments

- **NASA** and the **JWST team** for the incredible data
- **MAST Archive** (Mikulski Archive for Space Telescopes) for providing programmatic access
- **STScI** (Space Telescope Science Institute) for JWST operations
- **Railway** for easy and affordable hosting
- The **astroquery** team for making MAST data accessible via Python

## Contact

Questions or suggestions? 
- Open an issue on GitHub
- Submit a pull request
- Check out the `/docs` endpoint for interactive API exploration

## Resources

- [JWST Official Website](https://www.jwst.nasa.gov/)
- [MAST Archive](https://mast.stsci.edu/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [astroquery Documentation](https://astroquery.readthedocs.io/)

---

**Made with ❤️ for the astronomy community**
