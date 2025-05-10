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

    @token_manager.token_required
    def get_meeting_recordings(self, meeting_id: str, token: Optional[str]=None) -> Dict:
        """Get the recordings of a meeting."""
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            encoded_meeting_id = quote(meeting_id, safe='')
            url = f"{self.base_url}/meetings/{encoded_meeting_id}/recordings"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json().get('recording_files', [])
        except RequestException as e:
            self.logger.error(f"Error getting recording details for {encoded_meeting_id}: {e}")
            raise

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize the filename to ensure it is valid for both S3 and local disk storage."""
        invalid_chars = r'<>:"/\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Limit the length of the filename
        max_length = 255
        if len(filename) > max_length:
            filename = filename[:max_length]
        
        return filename

    @token_manager.token_required
    def download_meeting_recordings(
        self,
        download_url: str,
        email: str,
        start_time: str, # 2025-01-01T01:20:50Z UTC
        topic: str,
        file_extension: str,
        token: Optional[str]=None
    ) -> str:
        """Download the recording and upload to S3"""
        try:
            s3_client = boto3.client('s3')
            bucket_name = 'S3_BUCKET_NAME'

            # Sanitize and format the start time and topic
            sanitized_start_time = self.sanitize_filename(start_time.replace(':', '-'))
            sanitized_topic = self.sanitize_filename(topic)

            # Construct S3 key (path) for the recording
            s3_key = f"recordings/{email}/{sanitized_start_time}/{sanitized_topic}.{file_extension}"
            s3_url = f"s3://{bucket_name}/{s3_key}"

            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            self.logger.info(f"Starting download of recording {topic} from {download_url}")
            with requests.get(download_url, headers=headers, stream=True) as response:
                response.raise_for_status()

                # Use a temporary file to store the downloaded content
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    # Download in chunks (1MB) for memory efficiency
                    chunk_size = 1024 * 1024  # 1MB
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:  # Filter out keep-alive chunks
                            temp_file.write(chunk)
                    temp_file_path = temp_file.name

            # Upload the temporary file to S3
            self.logger.info(f"Uploading recording {topic} to S3: {s3_url}")

            try:
                s3_client.upload_file(
                    Filename=temp_file_path,
                    Bucket=bucket_name,
                    Key=s3_key,
                    ExtraArgs={
                        'ContentType': file_extension,
                        'ServerSideEncryption': 'AES256'  # Enable server-side encryption
                    }
                )
            finally:
                # Clean up the temporary file
                os.unlink(temp_file_path)
            
            # Verify the upload by checking if the object exists
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            self.logger.info(f"Successfully uploaded recording {topic} to {s3_url}")

            # Return the S3 URL for database storage
            return s3_url

        except RequestException as e:
            self.logger.error(f"Failed to download recording {topic} from Zoom: {e}")
            raise
        except ClientError as e:
            self.logger.error(f"Failed to upload recording {topic} to S3: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error for recording {topic}: {e}")
            raise

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