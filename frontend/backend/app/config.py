"""Configuration management for the FastAPI backend."""
import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # MotherDuck configuration
    motherduck_token: str
    motherduck_database: Optional[str] = None
    motherduck_schema: Optional[str] = "public"
    
    # API authentication
    api_key: str
    
    # Server configuration
    host: str = "0.0.0.0"
    port: int = 8000
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()

