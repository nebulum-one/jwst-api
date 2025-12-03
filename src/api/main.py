from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import Optional, List
import random

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
            "search": "/observations/search",
            "latest": "/observations/latest",
            "random": "/observations/random",
            "instruments": "/instruments",
            "targets": "/targets"
        }
    }


@app.get("/observations")
async def get_observations(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    instrument: Optional[str] = None,
    target: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get list of observations with optional filtering"""
    query = db.query(JWSTObservation)
    
    if instrument:
        query = query.filter(JWSTObservation.instrument.ilike(f"%{instrument}%"))
    
    if target:
        query = query.filter(JWSTObservation.target_name.ilike(f"%{target}%"))
    
    total = query.count()
    observations = query.order_by(desc(JWSTObservation.observation_date)).offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
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
    """Get list of all instruments"""
    instruments = db.query(JWSTObservation.instrument).distinct().all()
    
    return {
        "instruments": [inst[0] for inst in instruments if inst[0]]
    }


@app.get("/targets")
async def get_targets(
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get list of observed targets"""
    targets = db.query(JWSTObservation.target_name).distinct().limit(limit).all()
    
    return {
        "total": len(targets),
        "targets": [target[0] for target in targets if target[0]]
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
