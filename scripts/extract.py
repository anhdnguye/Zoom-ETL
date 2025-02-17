import requests
import logging
from datetime import datetime
from auth import token_manager
from airflow.models import Variable

class DataExtractor:
    def __init__(self, base_url):
        self.base_url = base_url
        self.token_manager = token_manager
    
    @token_manager.token_required
    def get_all_user_ids(self, token=None):
        """Get all user IDs from the API."""
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            url = f"{self.base_url}/users"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            users = response.json()
            return [user['id'] for user in users]
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting user IDs: {e}")
            raise
    
    @token_manager.token_required
    def get_user_details(self, user_id, token=None):
        """Get details for a specific user."""
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            url = f"{self.base_url}/users/{user_id}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting user details for {user_id}: {e}")
            raise
    
    @token_manager.token_required
    def get_user_meetings(self, user_id, since_timestamp, token=None):
        """Get meetings for a user since the specified timestamp."""
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            params = {'since': since_timestamp}
            url = f"{self.base_url}/users/{user_id}/meetings"
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting meetings for user {user_id}: {e}")
            raise

    def get_last_run_timestamp(self):
        """Get the timestamp of the last pipeline run."""
        try:
            last_run = Variable.get("last_pipeline_run", default_var=None)
            if last_run:
                return last_run
            return datetime.now().isoformat()
        except Exception as e:
            logging.error(f"Error getting last run timestamp: {e}")
            return datetime.now().isoformat()

    def set_last_run_timestamp(self):
        """Set the current timestamp as the last pipeline run."""
        try:
            Variable.set("last_pipeline_run", datetime.now().isoformat())
        except Exception as e:
            logging.error(f"Error setting last run timestamp: {e}")
            raise