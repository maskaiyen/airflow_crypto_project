import pytest
from unittest.mock import Mock, patch
from src.api_client import CoinGeckoClient, CoinGeckoAPIError
import json
import requests

class TestCoinGeckoClient:
    """Test CoinGeckoClient business logic"""

    @pytest.fixture
    def client(self):
        return CoinGeckoClient(timeout=10)

    @pytest.fixture
    def mock_api_response(self):
        return [
                {
                    'id': 'bitcoin', 
                    'symbol': 'btc', 
                    'name': 'Bitcoin', 
                    'image': 'https://coin-images.coingecko.com/coins/images/1/large/bitcoin.png?1696501400', 
                    'current_price': 93043, 
                    'market_cap': 1858305095863, 
                    'market_cap_rank': 1, 
                    'fully_diluted_valuation': 1858307979414, 
                    'total_volume': 43671338173, 
                    'high_24h': 95468, 
                    'low_24h': 92263, 
                    'price_change_24h': -2074.357513210838, 
                    'price_change_percentage_24h': -2.18084, 
                    'market_cap_change_24h': -40753566375.51294, 
                    'market_cap_change_percentage_24h': -2.14599, 
                    'circulating_supply': 19977962.0, 
                    'total_supply': 19977993.0, 
                    'max_supply': 21000000.0, 
                    'ath': 126080, 
                    'ath_change_percentage': -26.20318, 
                    'ath_date': '2025-10-06T18:57:42.558Z', 
                    'atl': 67.81, 
                    'atl_change_percentage': 137113.27782, 
                    'atl_date': '2013-07-06T00:00:00.000Z', 
                    'roi': None, 
                    'last_updated': '2026-01-19T16:04:33.996Z', 
                    'price_change_percentage_24h_in_currency': -2.180841251101688, 
                    'price_change_percentage_30d_in_currency': 5.481932378089368, 
                    'price_change_percentage_7d_in_currency': 1.6050987376127268
                }, 
                {
                    'id': 'ethereum', 
                    'symbol': 'eth', 
                    'name': 'Ethereum', 
                    'image': 'https://coin-images.coingecko.com/coins/images/279/large/ethereum.png?1696501628', 'current_price': 3214.98, 'market_cap': 387931240279, 
                    'market_cap_rank': 2, 
                    'fully_diluted_valuation': 387931240279, 
                    'total_volume': 30641480222, 
                    'high_24h': 3364.25, 
                    'low_24h': 3190.76, 
                    'price_change_24h': -119.47590655700606, 
                    'price_change_percentage_24h': -3.58307, 
                    'market_cap_change_24h': -14021119930.795776, 
                    'market_cap_change_percentage_24h': -3.48825, 
                    'circulating_supply': 120694585.0611229, 
                    'total_supply': 120694585.0611229, 
                    'max_supply': None, 
                    'ath': 4946.05, 
                    'ath_change_percentage': -34.99901, 
                    'ath_date': '2025-08-24T19:21:03.333Z', 
                    'atl': 0.432979, 
                    'atl_change_percentage': 742425.92918, 
                    'atl_date': '2015-10-20T00:00:00.000Z', 
                    'roi': {'times': 45.19295638735206, 'currency': 'btc', 'percentage': 4519.295638735206}, 
                    'last_updated': '2026-01-19T16:04:34.506Z', 
                    'price_change_percentage_24h_in_currency': -3.5830722752654136, 
                    'price_change_percentage_30d_in_currency': 7.943840193641289, 
                    'price_change_percentage_7d_in_currency': 2.790039037397611
                }
            ]

    def test_parameter_validation_per_page_too_large(self, client):
        """Test parameter validation: per_page exceeds maximum"""
        with pytest.raises(ValueError, match="per_page must be between 1-250"):
            client.get_markets_data(per_page=300)
    
    def test_parameter_validation_per_page_too_small(self, client):
        """Test parameter validation: per_page below minimum"""
        with pytest.raises(ValueError, match="per_page must be between 1-250"):
            client.get_markets_data(per_page=0)
    
    def test_parameter_validation_page_invalid(self, client):
        """Test parameter validation: invalid page"""
        with pytest.raises(ValueError, match="page must be greater than 0"):
            client.get_markets_data(page=-1)
    
    @patch('src.api_client.requests.get')
    def test_get_markets_data_success_with_default_params(self, mock_get, client, mock_api_response):
        """Test successful response"""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response
        
        # Execute test
        result = client.get_markets_data()
        
        # Verify results
        assert len(result) == 2
        assert result[0].id == 'bitcoin'
        assert result[0].symbol == 'btc'
        assert result[0].current_price == 93043
        
        # Verify call parameters
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs['timeout'] == 10
        assert 'headers' in call_kwargs
    
    @patch('src.api_client.requests.get')
    def test_get_markets_data_success_with_custom_params(self, mock_get, client, mock_api_response):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response

        result = client.get_markets_data(
            vs_currency='eur',
            per_page=50,
            page=2,
            price_change_percentage='1h,24h'
            )

        assert len(result) == 2
        call_params = mock_get.call_args.kwargs['params']
        assert call_params['vs_currency'] == 'eur'
        assert call_params['per_page'] == 50
        assert call_params['page'] == 2
        assert call_params['price_change_percentage'] == '1h,24h'


    @patch('src.api_client.requests.get')
    def test_get_markets_data_return_empty_list(self, mock_get, client):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        result = client.get_markets_data(page=999)

        assert result == []
        assert isinstance(result, list)

    @patch('src.api_client.requests.get')
    def test_rate_limit_429(self, mock_get, client):
        """Test exception raised when rate limited (429)"""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers.get.return_value = '60'
        mock_get.return_value = mock_response
        
        with pytest.raises(CoinGeckoAPIError, match="rate limit"):
            client.get_markets_data()

    @patch('src.api_client.requests.get')
    def test_bad_request_400_raises_exception(self, mock_get, client):
        """Test server error (400)"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = 'Invalid parameters'
        mock_get.return_value = mock_response

        with pytest.raises(CoinGeckoAPIError, match="HTTP 400"):
            client.get_markets_data()
    
    @patch('src.api_client.requests.get')
    def test_server_error_500(self, mock_get, client):
        """Test server error (500)"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response
        
        with pytest.raises(CoinGeckoAPIError, match="HTTP 500"):
            client.get_markets_data()
    
    @patch('src.api_client.requests.get')
    def test_timeout_exception(self, mock_get, client):
        """Test request timeout"""
        mock_get.side_effect = requests.exceptions.Timeout()
        
        with pytest.raises(CoinGeckoAPIError, match="Request timeout"):
            client.get_markets_data()
    
    @patch('src.api_client.requests.get')
    def test_network_error(self, mock_get, client):
        """Test network error"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")
        
        with pytest.raises(CoinGeckoAPIError, match="Request error"):
            client.get_markets_data()
    
    @patch('src.api_client.requests.get')
    def test_request_headers_are_set(self, mock_get, client):
        """Test request headers are set correctly"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        client.get_markets_data()
        
        call_kwargs = mock_get.call_args.kwargs
        headers = call_kwargs['headers']
        assert headers['Accept'] == 'application/json'
        assert 'User-Agent' in headers