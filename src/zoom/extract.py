import requests
import logging
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional, Generator
from requests.exceptions import RequestException, HTTPError
from zoom.oauth import token_manager
from airflow.sdk import Variable
from urllib.parse import quote

from errors.error_handler import PipelineErrorHandler, retryable
from errors.error_types import ErrorType

# Setup error handler
handler = PipelineErrorHandler(
    admin_email="admin@example.com",
    dag_id="zoom_pipeline"
)

class DataExtractor:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')
        self.token_manager = token_manager
        self.logger = logging.getLogger(__name__)
        self.DEFAULT_PAGE_SIZE = 300

    def _make_paginated_request(self, url: str, headers: Dict, params: Dict = None) -> Generator[Dict, None, None]:
        """Helper method to handle paginated API requests."""
        params = params or {}
        while True:
            try:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limit hit, retrying after {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                data = response.json()
                yield data
                
                if not data.get('next_page_token'):
                    break
                params['next_page_token'] = data['next_page_token']
            except RequestException as e:
                self.logger.error(f"API request failed for {url}: {e}, response: {response.text if response else 'No response'}")
                raise

    @token_manager.token_required
    def get_all_user_ids(self, token: Optional[str] = None) -> List[str]:
        """Get all user IDs from the API."""
        user_emails = []
        for _status in {'active', 'inactive'}:
            headers = {
                'Authorization' : f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            url = f"{self.base_url}/users"
            params = {
                'page_size' : self.DEFAULT_PAGE_SIZE,
                'status' : _status
            }

            for page in self._make_paginated_request(url, headers, params):
                user_emails.extend(user['email'] for user in page.get('users', []))
            
        return list(set(user_emails))
    
    @retryable(handler=handler, retries=3, delay=2, error_type=ErrorType.API_ERROR)
    @token_manager.token_required
    def get_user_details(self, user_id: str, token: Optional[str]=None) -> Dict:
        """Get details for a specific user."""
        headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
        url = f"{self.base_url}/users/{user_id}"

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        
        except HTTPError as http_err:
            status = http_err.response.status_code if http_err.response else "Unknown"
            if status == 404:
                self.logger.warning(f"Meeting not found (404): {url}")
            else:
                self.logger.error(f"HTTP error occurred while accessing {url}: {http_err}")
            raise  # Important: re-raise so retry can occur

        except RequestException as req_err:
            self.logger.error(f"Error getting user details for {user_id}: {req_err}")
            raise
    
    def getRange(self, start: datetime, end: datetime) -> Generator[tuple[datetime, datetime], None, None]:
        """Generate date ranges in 30-day chunks."""
        curr = start
        date_difference = timedelta(days=30)
        while curr < end:
            yield curr, min(curr + date_difference, end)
            curr += date_difference

    @token_manager.token_required
    def get_meetings(
        self,
        user_id: str,
        since_timestamp: datetime,
        token: Optional[str]=None
    ) -> List[str]:
        """Get meetings for a user since the specified timestamp."""
        url = f"{self.base_url}/report/users/{user_id}/meetings"

        lstMeetings = []
        for start, end in self.getRange(since_timestamp, datetime.today()):
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            params = {
                'from': start.strftime('%Y-%m-%d'),
                'to':end.strftime('%Y-%m-%d'),
                'page_size': self.DEFAULT_PAGE_SIZE
            }

            for page in self._make_paginated_request(url, headers, params):
                lstMeetings.extend(meeting['uuid'] for meeting in page.get('meetings', []))
        return lstMeetings

    @retryable(handler=handler, retries=3, delay=2, error_type=ErrorType.API_ERROR)
    @token_manager.token_required
    def get_meeting_details(self, meeting_id: str, token: Optional[str]=None) -> Dict:
        """Get details for a specific meeting using both metrics and meetings endpoints."""
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        encoded_meeting_id = quote(quote(meeting_id, safe=''), safe='')
        url = f"{self.base_url}/past_meetings/{encoded_meeting_id}"

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        
        except HTTPError as http_err:
            status = http_err.response.status_code if http_err.response else "Unknown"
            if status == 404:
                self.logger.warning(f"Meeting not found (404): {url}")
            else:
                self.logger.error(f"HTTP error occurred while accessing {url}: {http_err}")
            raise  # Important: re-raise so retry can occur

        except RequestException as req_err:
            self.logger.error(f"Failed to get meeting details from {url}: {req_err}")

    @token_manager.token_required
    def get_meeting_participants(self, meeting_id: str, token: Optional[str]=None) -> List[Dict]:
        """Get a list of participants of a meeting."""
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        encoded_meeting_id = quote(quote(meeting_id, safe=''), safe='')
        url = f"{self.base_url}/past_meetings/{encoded_meeting_id}/participants"
        params = {'page_size': self.DEFAULT_PAGE_SIZE}
        
        participants = []
        for page in self._make_paginated_request(url, headers, params):
            participants.extend(page.get('participants', []))

        return participants

    def get_last_run_timestamp(self) -> str:
        """Get the timestamp of the last pipeline run."""
        try:
            last_run = Variable.get("last_pipeline_run")
            return last_run if last_run else datetime.now().isoformat()
        except Exception as e:
            logging.error(f"Error getting last run timestamp: {e}")
            return datetime.now().isoformat()

    def set_last_run_timestamp(self) -> None:
        """Set the current timestamp as the last pipeline run."""
        try:
            Variable.set("last_pipeline_run", datetime.now().isoformat())
        except Exception as e:
            logging.error(f"Error setting last run timestamp: {e}")
            raise