from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_
from typing import Optional, List
from datetime import datetime, timedelta
import random
import math

from src.db.database import get_db, init_db
from src.db.models import JWSTObservation

# Initialize database on startup
init_db()

app = FastAPI(
    title="JWST API",
    description="REST API for accessing James Webb Space Telescope observation data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "JWST API",
        "version": "1.0.0",
        "description": "REST API for James Webb Space Telescope observations",
        "documentation": "/docs",
        "endpoints": {
            "observations": "/observations",
            "observation_by_id": "/observations/{obs_id}",
            "search": "/observations/search",
            "search_by_coordinates": "/observations/search/coordinates",
            "search_by_date": "/observations/search/date",
            "latest": "/observations/latest",
            "random": "/observations/random",
            "instruments": "/instruments",
            "filters": "/filters",
            "targets": "/targets",
            "proposals": "/proposals",
            "statistics": "/statistics"
        }
    }


@app.get("/observations")
async def get_observations(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    instrument: Optional[str] = None,
    target: Optional[str] = None,
    filter: Optional[str] = None,
    proposal_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get list of observations with optional filtering"""
    query = db.query(JWSTObservation)
    
    if instrument:
        query = query.filter(JWSTObservation.instrument.ilike(f"%{instrument}%"))
    
    if target:
        query = query.filter(JWSTObservation.target_name.ilike(f"%{target}%"))
    
    if filter:
        query = query.filter(JWSTObservation.filter_name.ilike(f"%{filter}%"))
    
    if proposal_id:
        query = query.filter(JWSTObservation.proposal_id == proposal_id)
    
    total = query.count()
    observations = query.order_by(desc(JWSTObservation.observation_date)).offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "filters_applied": {
            "instrument": instrument,
            "target": target,
            "filter": filter,
            "proposal_id": proposal_id
        },
        "results": [obs.to_dict() for obs in observations]
    }


@app.get("/observations/search")
async def search_observations(
    q: Optional[str] = Query(None, description="Search query for target name or description"),
    instrument: Optional[str] = Query(None, description="Filter by instrument (e.g., NIRCAM, MIRI)"),
    filter: Optional[str] = Query(None, description="Filter by filter name (e.g., F200W)"),
    proposal_id: Optional[str] = Query(None, description="Filter by proposal ID"),
    target_classification: Optional[str] = Query(None, description="Filter by target type"),
    calib_level: Optional[int] = Query(None, ge=1, le=3, description="Calibration level (1-3)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Advanced search with multiple filters.
    Searches across target names, descriptions, and applies various filters.
    """
    query = db.query(JWSTObservation)
    
    # Text search across target name and description
    if q:
        query = query.filter(
            or_(
                JWSTObservation.target_name.ilike(f"%{q}%"),
                JWSTObservation.description.ilike(f"%{q}%")
            )
        )
    
    # Apply filters
    if instrument:
        query = query.filter(JWSTObservation.instrument.ilike(f"%{instrument}%"))
    
    if filter:
        query = query.filter(JWSTObservation.filter_name.ilike(f"%{filter}%"))
    
    if proposal_id:
        query = query.filter(JWSTObservation.proposal_id == proposal_id)
    
    if target_classification:
        query = query.filter(JWSTObservation.target_classification.ilike(f"%{target_classification}%"))
    
    if calib_level:
        query = query.filter(JWSTObservation.calib_level == calib_level)
    
    total = query.count()
    observations = query.order_by(desc(JWSTObservation.observation_date)).offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "query": q,
        "filters_applied": {
            "instrument": instrument,
            "filter": filter,
            "proposal_id": proposal_id,
            "target_classification": target_classification,
            "calib_level": calib_level
        },
        "results": [obs.to_dict() for obs in observations]
    }


@app.get("/observations/search/coordinates")
async def search_by_coordinates(
    ra: float = Query(..., description="Right Ascension in degrees (0-360)"),
    dec: float = Query(..., description="Declination in degrees (-90 to 90)"),
    radius: float = Query(1.0, ge=0.001, le=10.0, description="Search radius in degrees"),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Cone search: Find observations near a specific sky coordinate.
    Uses a simple Euclidean distance approximation for small angles.
    
    Example: /observations/search/coordinates?ra=202.5&dec=47.3&radius=0.5
    """
    # Get all observations (in production, you'd want spatial indexing)
    all_obs = db.query(JWSTObservation).filter(
        JWSTObservation.ra.isnot(None),
        JWSTObservation.dec.isnot(None)
    ).all()
    
    # Calculate angular distance for each observation
    matches = []
    for obs in all_obs:
        # Simple angular distance calculation (works for small angles)
        # For production, use proper spherical geometry
        delta_ra = (obs.ra - ra) * math.cos(math.radians(dec))
        delta_dec = obs.dec - dec
        distance = math.sqrt(delta_ra**2 + delta_dec**2)
        
        if distance <= radius:
            obs_dict = obs.to_dict()
            obs_dict['angular_distance'] = round(distance, 6)
            matches.append(obs_dict)
    
    # Sort by distance
    matches.sort(key=lambda x: x['angular_distance'])
    
    return {
        "search_center": {
            "ra": ra,
            "dec": dec
        },
        "radius_degrees": radius,
        "total_found": len(matches),
        "results": matches[:limit]
    }


@app.get("/observations/search/date")
async def search_by_date(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    days_ago: Optional[int] = Query(None, ge=1, description="Observations from last N days"),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Search observations by date range.
    
    Examples:
    - /observations/search/date?start_date=2024-01-01&end_date=2024-12-31
    - /observations/search/date?days_ago=30
    """
    query = db.query(JWSTObservation).filter(JWSTObservation.observation_date.isnot(None))
    
    # Use days_ago if provided, otherwise use date range
    if days_ago:
        cutoff_date = datetime.utcnow() - timedelta(days=days_ago)
        query = query.filter(JWSTObservation.observation_date >= cutoff_date)
    else:
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date)
                query = query.filter(JWSTObservation.observation_date >= start_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD")
        
        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date)
                query = query.filter(JWSTObservation.observation_date <= end_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD")
    
    total = query.count()
    observations = query.order_by(desc(JWSTObservation.observation_date)).offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date,
            "days_ago": days_ago
        },
        "results": [obs.to_dict() for obs in observations]
    }


@app.get("/observations/{obs_id}")
async def get_observation(obs_id: str, db: Session = Depends(get_db)):
    """Get a specific observation by ID"""
    observation = db.query(JWSTObservation).filter(JWSTObservation.obs_id == obs_id).first()
    
    if not observation:
        raise HTTPException(status_code=404, detail="Observation not found")
    
    return observation.to_dict()


@app.get("/observations/latest")
async def get_latest_observations(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get the most recent observations"""
    observations = db.query(JWSTObservation).order_by(
        desc(JWSTObservation.observation_date)
    ).limit(limit).all()
    
    return {
        "total": len(observations),
        "results": [obs.to_dict() for obs in observations]
    }


@app.get("/observations/random")
async def get_random_observation(db: Session = Depends(get_db)):
    """Get a random observation"""
    count = db.query(func.count(JWSTObservation.id)).scalar()
    
    if count == 0:
        raise HTTPException(status_code=404, detail="No observations found in database")
    
    random_offset = random.randint(0, count - 1)
    observation = db.query(JWSTObservation).offset(random_offset).first()
    
    return observation.to_dict()


@app.get("/instruments")
async def get_instruments(db: Session = Depends(get_db)):
    """Get list of all instruments with observation counts"""
    instruments = db.query(
        JWSTObservation.instrument,
        func.count(JWSTObservation.id).label('count')
    ).filter(
        JWSTObservation.instrument.isnot(None),
        JWSTObservation.instrument != ''
    ).group_by(JWSTObservation.instrument).all()
    
    return {
        "total": len(instruments),
        "instruments": [
            {"name": inst[0], "observation_count": inst[1]} 
            for inst in instruments
        ]
    }


@app.get("/filters")
async def get_filters(
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get list of all filters with observation counts"""
    filters = db.query(
        JWSTObservation.filter_name,
        func.count(JWSTObservation.id).label('count')
    ).filter(
        JWSTObservation.filter_name.isnot(None),
        JWSTObservation.filter_name != ''
    ).group_by(JWSTObservation.filter_name).order_by(desc('count')).limit(limit).all()
    
    return {
        "total": len(filters),
        "filters": [
            {"name": filt[0], "observation_count": filt[1]} 
            for filt in filters
        ]
    }


@app.get("/targets")
async def get_targets(
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get list of observed targets with observation counts"""
    targets = db.query(
        JWSTObservation.target_name,
        func.count(JWSTObservation.id).label('count')
    ).filter(
        JWSTObservation.target_name.isnot(None),
        JWSTObservation.target_name != ''
    ).group_by(JWSTObservation.target_name).order_by(desc('count')).limit(limit).all()
    
    return {
        "total": len(targets),
        "targets": [
            {"name": target[0], "observation_count": target[1]} 
            for target in targets
        ]
    }


@app.get("/proposals")
async def get_proposals(
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get list of all proposals with observation counts and details"""
    proposals = db.query(
        JWSTObservation.proposal_id,
        JWSTObservation.description,
        JWSTObservation.pi_name,
        func.count(JWSTObservation.id).label('count')
    ).filter(
        JWSTObservation.proposal_id.isnot(None),
        JWSTObservation.proposal_id != ''
    ).group_by(
        JWSTObservation.proposal_id,
        JWSTObservation.description,
        JWSTObservation.pi_name
    ).order_by(desc('count')).limit(limit).all()
    
    return {
        "total": len(proposals),
        "proposals": [
            {
                "proposal_id": prop[0],
                "description": prop[1],
                "pi_name": prop[2],
                "observation_count": prop[3]
            }
            for prop in proposals
        ]
    }


@app.get("/proposals/{proposal_id}")
async def get_proposal_observations(
    proposal_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Get all observations for a specific proposal"""
    query = db.query(JWSTObservation).filter(JWSTObservation.proposal_id == proposal_id)
    
    total = query.count()
    
    if total == 0:
        raise HTTPException(status_code=404, detail="Proposal not found")
    
    observations = query.order_by(desc(JWSTObservation.observation_date)).offset(skip).limit(limit).all()
    
    # Get proposal details from first observation
    first_obs = observations[0] if observations else None
    
    return {
        "proposal_id": proposal_id,
        "description": first_obs.description if first_obs else None,
        "pi_name": first_obs.pi_name if first_obs else None,
        "total_observations": total,
        "skip": skip,
        "limit": limit,
        "results": [obs.to_dict() for obs in observations]
    }


@app.get("/statistics")
async def get_statistics(db: Session = Depends(get_db)):
    """Get comprehensive statistics about the JWST observations database"""
    
    total_observations = db.query(func.count(JWSTObservation.id)).scalar()
    
    # Get date range
    date_range = db.query(
        func.min(JWSTObservation.observation_date),
        func.max(JWSTObservation.observation_date)
    ).first()
    
    # Get instrument counts
    instrument_stats = db.query(
        JWSTObservation.instrument,
        func.count(JWSTObservation.id).label('count')
    ).filter(
        JWSTObservation.instrument.isnot(None)
    ).group_by(JWSTObservation.instrument).all()
    
    # Get most observed targets
    top_targets = db.query(
        JWSTObservation.target_name,
        func.count(JWSTObservation.id).label('count')
    ).filter(
        JWSTObservation.target_name.isnot(None),
        JWSTObservation.target_name != ''
    ).group_by(JWSTObservation.target_name).order_by(desc('count')).limit(10).all()
    
    # Get most used filters
    top_filters = db.query(
        JWSTObservation.filter_name,
        func.count(JWSTObservation.id).label('count')
    ).filter(
        JWSTObservation.filter_name.isnot(None),
        JWSTObservation.filter_name != ''
    ).group_by(JWSTObservation.filter_name).order_by(desc('count')).limit(10).all()
    
    # Get total exposure time
    total_exposure = db.query(func.sum(JWSTObservation.exposure_time)).scalar() or 0
    
    # Get unique counts
    unique_targets = db.query(func.count(func.distinct(JWSTObservation.target_name))).scalar()
    unique_proposals = db.query(func.count(func.distinct(JWSTObservation.proposal_id))).scalar()
    
    return {
        "overview": {
            "total_observations": total_observations,
            "unique_targets": unique_targets,
            "unique_proposals": unique_proposals,
            "total_exposure_time_seconds": round(total_exposure, 2),
            "total_exposure_time_hours": round(total_exposure / 3600, 2),
            "date_range": {
                "earliest": date_range[0].isoformat() if date_range[0] else None,
                "latest": date_range[1].isoformat() if date_range[1] else None
            }
        },
        "instruments": {
            "total_instruments": len(instrument_stats),
            "breakdown": [
                {"name": inst[0], "count": inst[1], "percentage": round(inst[1] / total_observations * 100, 1)}
                for inst in instrument_stats
            ]
        },
        "top_targets": [
            {"name": target[0], "observation_count": target[1]}
            for target in top_targets
        ],
        "top_filters": [
            {"name": filt[0], "observation_count": filt[1]}
            for filt in top_filters
        ]
    }


@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint with database status"""
    try:
        # Test database connection
        count = db.query(func.count(JWSTObservation.id)).scalar()
        return {
            "status": "healthy",
            "database": "connected",
            "total_observations": count
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }