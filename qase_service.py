"""
Qase API Service - Handles initialization and configuration of Qase API clients.
"""
from qase.api_client_v1.api_client import ApiClient
from qase.api_client_v1.configuration import Configuration
from qase.api_client_v2.api_client import ApiClient as ApiClientV2
from qase.api_client_v2.configuration import Configuration as ConfigurationV2
import certifi


class QaseService:
    """Service class for interacting with Qase API."""
    
    def __init__(self, api_token: str, host: str = "qase.io", ssl: bool = True, enterprise: bool = False):
        """
        Initialize Qase API clients.
        
        Args:
            api_token: Qase API token
            host: Qase host (default: "qase.io")
            ssl: Use SSL (default: True)
            enterprise: Is enterprise instance (default: False)
        """
        self.api_token = api_token
        self.host = host
        self.ssl = ssl
        self.enterprise = enterprise
        
        # Determine API host format
        # Cloud: api.qase.io/v1
        # Enterprise custom domain: api-{host}/v1
        ssl_prefix = 'https://' if ssl else 'http://'
        delimiter = '.' if not enterprise or host == 'qase.io' else '-'
        
        api_host_v1 = f'{ssl_prefix}api{delimiter}{host}/v1'
        api_host_v2 = f'{ssl_prefix}api{delimiter}{host}/v2'
        
        # Configure API v1 client
        configuration = Configuration()
        configuration.api_key['TokenAuth'] = api_token
        configuration.host = api_host_v1
        configuration.ssl_ca_cert = certifi.where()
        self.client = ApiClient(configuration)
        
        # Configure API v2 client
        configuration_v2 = ConfigurationV2()
        configuration_v2.api_key['TokenAuth'] = api_token
        configuration_v2.host = api_host_v2
        configuration_v2.ssl_ca_cert = certifi.where()
        self.client_v2 = ApiClientV2(configuration_v2)
        
        # Add custom header for migration
        self.client_v2.default_headers['migration'] = 'true'
