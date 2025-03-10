import requests
import logging
from datetime import datetime, timedelta
from oauth import token_manager
from airflow.models import Variable

class DataExtractor:
    def __init__(self, base_url):
        self.base_url = base_url
        self.token_manager = token_manager
    
    @token_manager.token_required
    def get_all_user_ids(self, token=None):
        """Get all user IDs from the API."""
        user_email = []
        for _status in {'active', 'inactive'}:
            try:
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json'
                }
                
                url = f"{self.base_url}/users"
                params = {
                    'page_size' : 300,
                    'status' : _status,
                }
                while True:
                    response = requests.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    
                    users = response.json()
                    user_email.extend([user['email'] for user in users['users']])
                    if not users['next_page_token']:
                        break
                    params['next_page_token'] = users['next_page_token']
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Error getting user IDs: {e}")
                raise
        return user_email
    
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
    
    # Get the time range
    def getRange(self, start, end):
        curr = start
        date_difference = timedelta(days=30)
        while curr < end:
            yield curr, min(curr + date_difference, end)
            curr += date_difference

    @token_manager.token_required
    def get_meetings(self, user_id, since_timestamp, token=None):
        """Get meetings for a user since the specified timestamp. Use /report/users/{userId}/meetings"""
        lstMeetings = []
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            url = f"{self.base_url}/report/users/{user_id}/meetings"
            for start, end in self.getRange(since_timestamp, datetime.today()):    
                params = {
                    'from': start.strftime('%Y-%m-%d'),
                    'to':end.strftime('%Y-%m-%d'),
                }
                while True:
                    response = requests.get(url, headers=headers, params=params)
                    response.raise_for_status()

                    meetings = response.json()
                    lstMeetings.extend([meeting['uuid'] for meeting in meetings['meetings']])
                    if not meetings['next_page_token']:
                        break
                    params['next_page_token'] = meetings['next_page_token']

        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting meetings for user {user_id}: {e}")
            raise
        return lstMeetings

    @token_manager.token_required
    def get_meeting_details(self, meeting_id, token=None):
        """Get details for a specific meeting"""
        pass

    @token_manager.token_required
    def get_meeting_participants(self, meeting_id, token=None):
        """Get a list of participants of a meeting"""
        pass

    @token_manager.token_required
    def get_meeting_recordings(self, meeting_id, token=None):
        """Get the recordings of a meeting"""
        pass

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