import os
import requests
import logging
from datetime import datetime, timedelta
import time
from typing import List, Dict, Optional, Generator
from requests.exceptions import RequestException
from oauth import token_manager
from airflow.sdk import Variable
from urllib.parse import quote
import tempfile

import boto3
from botocore.exceptions import ClientError

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
    
    @token_manager.token_required
    def get_user_details(self, user_id: str, token: Optional[str]=None) -> Dict:
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
        except RequestException as e:
            self.logger.error(f"Error getting user details for {user_id}: {e}")
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

    @token_manager.token_required
    def get_meeting_details(self, meeting_id: str, token: Optional[str]=None) -> Dict:
        """Get details for a specific meeting using both metrics and meetings endpoints."""
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        encoded_meeting_id = quote(meeting_id, safe='')
        combined_details = {}
        endpoints = [
            f"{self.base_url}/metrics/meetings/{encoded_meeting_id}",
            f"{self.base_url}/meetings/{encoded_meeting_id}"
        ]

        for url in endpoints:
            try:
                for data in self._make_paginated_request(url, headers):
                    combined_details.update(data)
            except RequestException as e:
                self.logger.error(f"Failed to get meeting details from {url}: {e}")
                continue

        return combined_details

    @token_manager.token_required
    def get_meeting_participants(self, meeting_id: str, token: Optional[str]=None) -> List[Dict]:
        """Get a list of participants of a meeting."""
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        encoded_meeting_id = quote(meeting_id, safe='')
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