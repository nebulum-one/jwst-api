from sqlalchemy import Column, Integer, String, DateTime, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class JWSTObservation(Base):
    """Model for storing JWST observation metadata"""
    __tablename__ = 'observations'
    
    id = Column(Integer, primary_key=True, index=True)
    obs_id = Column(String, unique=True, index=True, nullable=False)
    target_name = Column(String, index=True)
    ra = Column(Float)  # Right Ascension
    dec = Column(Float)  # Declination
    instrument = Column(String, index=True)
    filter_name = Column(String)
    observation_date = Column(DateTime, index=True)
    preview_url = Column(Text)
    fits_url = Column(Text)
    description = Column(Text)
    proposal_id = Column(String)
    exposure_time = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert model to dictionary"""
        return {
            'id': self.id,
            'obs_id': self.obs_id,
            'target_name': self.target_name,
            'coordinates': {
                'ra': self.ra,
                'dec': self.dec
            },
            'instrument': self.instrument,
            'filter': self.filter_name,
            'observation_date': self.observation_date.isoformat() if self.observation_date else None,
            'preview_url': self.preview_url,
            'fits_url': self.fits_url,
            'description': self.description,
            'proposal_id': self.proposal_id,
            'exposure_time': self.exposure_time,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    