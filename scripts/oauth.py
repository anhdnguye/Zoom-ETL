import requests
import time
import threading
import logging
from datetime import datetime, timedelta
from airflow.models import Variable
from functools import wraps

class TokenManager:
    def __init__(self, token_endpoint, client_id_var, client_secret_var):
        self.token_endpoint = token_endpoint
        self.client_id_var = client_id_var
        self.client_secret_var = client_secret_var
        self.token = None
        self.expiry_time = None
        self.lock = threading.Lock()
        
    def _refresh_token(self):
        """Refresh the API token using client credentials."""
        try:
            client_id = Variable.get(self.client_id_var)
            client_secret = Variable.get(self.client_secret_var)
            
            payload = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret
            }
            
            response = requests.post(self.token_endpoint, data=payload)
            response.raise_for_status()
            
            token_data = response.json()
            self.token = token_data['access_token']
            # Set expiry time to 55 minutes (5 minutes buffer)
            self.expiry_time = datetime.now() + timedelta(minutes=55)
            
            logging.info("Token refreshed successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to refresh token: {e}")
            raise
    
    def get_token(self):
        """Get the current valid token, refreshing if necessary."""
        with self.lock:
            if not self.token or datetime.now() >= self.expiry_time:
                self._refresh_token()
            return self.token
    
    @staticmethod
    def token_required(func):
        """Decorator to ensure a valid token is available for API calls."""
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Access the token_manager from the instance (self)
            token = self.token_manager.get_token()
            return func(self, *args, token=token, **kwargs)
        return wrapper

# Initialize the token manager (replace with your actual token endpoint)
token_manager = TokenManager(
    token_endpoint="https://api.example.com/oauth/token",
    client_id_var="API_CLIENT_ID",
    client_secret_var="API_CLIENT_SECRET"
)