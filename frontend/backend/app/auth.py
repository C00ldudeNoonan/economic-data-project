"""API key authentication for FastAPI routes."""
from fastapi import Header, HTTPException, Security
from fastapi.security import APIKeyHeader
from .config import settings


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """
    Verify the API key from the request header.
    
    Args:
        api_key: API key from X-API-Key header
        
    Returns:
        str: The API key if valid
        
    Raises:
        HTTPException: If API key is missing or invalid
    """
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="API key is missing. Please provide X-API-Key header."
        )
    
    if api_key != settings.api_key:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key."
        )
    
    return api_key

