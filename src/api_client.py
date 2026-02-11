import logging
import time
from typing import List

import requests

from .schemas import CryptoDataSchema

logger = logging.getLogger("api_client")


class CoinGeckoAPIError(Exception):
    """Raised when the CoinGecko API call fails."""


class CoinGeckoClient:
    """Thin client wrapper for CoinGecko's /coins/markets endpoint."""
    
    BASE_URL = "https://api.coingecko.com/api/v3"
    DEFAULT_TIMEOUT = 30
    DEFAULT_RETRY_WAIT = 60

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout
    
    def get_markets_data(
        self, 
        vs_currency: str = 'usd',
        per_page: int = 250,
        page: int = 1,
        price_change_percentage: str = '24h,7d,30d'
    ) -> List[CryptoDataSchema]:
        """
        Fetch market data and return typed records.

        Args:
            vs_currency: Quote currency (e.g. 'usd').
            per_page: Number of records per page (1–250).
            page: Page index (1-based).
            price_change_percentage: Comma-separated periods requested from the API.

        Returns:
            A list of `CryptoDataSchema` instances parsed from the API response.

        Raises:
            ValueError: If input parameters are invalid.
            CoinGeckoAPIError: If the HTTP request fails, times out, or returns a non-200 status.
        """
        
        # parameter validation
        if not 1 <= per_page <= 250:
            raise ValueError("per_page must be between 1-250")
        if page < 1:
            raise ValueError("page must be greater than 0")
        
        url = f"{self.BASE_URL}/coins/markets"
        params = {
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': per_page,
            'page': page,
            'sparkline': False,
            'price_change_percentage': price_change_percentage
        }

        headers = {
            'Accept': 'application/json',
            'User-Agent': 'CoinGeckoClient/1.0'
        }
        
        try:            
            response = requests.get(
                url, 
                params=params, 
                headers=headers,
                timeout=self.timeout
            )
            
            logger.info(f"API Response: Status {response.status_code}")
                       
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', self.DEFAULT_RETRY_WAIT))
                logger.warning(f"Rate limited, waiting {retry_after} seconds before retry")
                raise CoinGeckoAPIError(f"API rate limit (429), Retry-After: {retry_after} seconds")
            
                
            if response.status_code != 200:
                error_msg = f"API error: HTTP {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise CoinGeckoAPIError(error_msg)
             
            json_data = response.json()
            market_data = [CryptoDataSchema(**item) for item in json_data]

            logger.info(f"Successfully fetched {len(market_data)} cryptocurrency records")
            return market_data
                
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout")
            raise CoinGeckoAPIError("Request timeout")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            raise CoinGeckoAPIError(f"Request error: {str(e)}")