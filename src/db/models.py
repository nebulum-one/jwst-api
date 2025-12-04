from sqlalchemy import Column, Integer, String, DateTime, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class JWSTObservation(Base):
    """Model for storing JWST observation metadata (images and spectra)"""
    __tablename__ = 'observations'
    
    id = Column(Integer, primary_key=True, index=True)
    obs_id = Column(String, unique=True, index=True, nullable=False)
    target_name = Column(String, index=True)
    ra = Column(Float)  # Right Ascension
    dec = Column(Float)  # Declination
    instrument = Column(String, index=True)
    filter_name = Column(String, index=True)
    observation_date = Column(DateTime, index=True)
    preview_url = Column(Text)
    fits_url = Column(Text)
    description = Column(Text)
    proposal_id = Column(String, index=True)
    exposure_time = Column(Float)
    
    # General metadata fields
    dataproduct_type = Column(String, index=True)  # image, spectrum, etc.
    calib_level = Column(Integer)  # 1=raw, 2=calibrated, 3=science-ready
    wavelength_region = Column(String)  # Infrared, optical, etc.
    pi_name = Column(String)  # Principal Investigator
    target_classification = Column(String)  # galaxy, star, exoplanet, etc.
    
    # Spectrum-specific metadata fields
    spectral_resolution = Column(Float)  # R = λ/Δλ
    wavelength_min = Column(Float)  # Minimum wavelength in microns
    wavelength_max = Column(Float)  # Maximum wavelength in microns
    dispersion_axis = Column(Integer)  # 1 or 2 for spectral dispersion direction
    grating = Column(String)  # Grating/disperser used (e.g., G140M, G235H)
    slit_width = Column(Float)  # Slit width in arcseconds
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert model to dictionary with MAST URL conversion"""
        # Helper to convert MAST URIs
        def convert_url(uri):
            if uri and uri.startswith("mast:"):
                return f"https://mast.stsci.edu/api/v0.1/Download/file?uri={uri}"
            return uri
        
        base_dict = {
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
            'preview_url': convert_url(self.preview_url),
            'fits_url': convert_url(self.fits_url),
            'description': self.description,
            'proposal_id': self.proposal_id,
            'exposure_time': self.exposure_time,
            'dataproduct_type': self.dataproduct_type,
            'calib_level': self.calib_level,
            'wavelength_region': self.wavelength_region,
            'pi_name': self.pi_name,
            'target_classification': self.target_classification,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
        
        # Add spectrum-specific fields if this is a spectrum
        if self.dataproduct_type == 'spectrum':
            base_dict['spectrum_metadata'] = {
                'spectral_resolution': self.spectral_resolution,
                'wavelength_range': {
                    'min': self.wavelength_min,
                    'max': self.wavelength_max,
                    'unit': 'microns'
                } if self.wavelength_min or self.wavelength_max else None,
                'dispersion_axis': self.dispersion_axis,
                'grating': self.grating,
                'slit_width': self.slit_width
            }
        
        return base_dict  
