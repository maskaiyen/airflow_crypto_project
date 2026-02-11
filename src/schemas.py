from pydantic import BaseModel
from typing import Optional
from typing_extensions import TypedDict

class CryptoDataSchema(BaseModel):
    """
    Response format for CoinGecko API /coins/markets endpoint.
    
    Purpose:
    - Validate API response structure
    - Ensure required fields exist
    - Filter out unnecessary extra fields
    
    Reference: https://docs.coingecko.com/reference/coins-markets
    """
    
    # Basic information
    id: str
    symbol: str
    name: str

    # Price information
    current_price: Optional[float]
    high_24h: Optional[float]
    low_24h: Optional[float]
    price_change_24h: Optional[float]
    
    # Market cap information
    market_cap: Optional[float]
    market_cap_rank: Optional[int]
    market_cap_change_24h: Optional[float]
    market_cap_change_percentage_24h: Optional[float]
    
    # Trading volume
    total_volume: Optional[float]
    
    # Supply information
    circulating_supply: Optional[float]
    total_supply: Optional[float]
    max_supply: Optional[float]
    
    # Price changes
    price_change_percentage_24h: Optional[float]

    class Config:
        extra = "ignore"   # Ignore extra fields