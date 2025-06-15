import os
import re
import json
import dropbox
from dropbox.exceptions import ApiError
from typing import Dict, Optional
from datetime import datetime
import pytz
import psycopg2
import boto3
import logging
import traceback
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_secret_cache = {}

class ErrorHandler(Exception):
    def __init__(self, message, error_type=None, details=None):
        self.message = message
        self.error_tpe = error_type
        self.details = details
        super().__init__(self.message)

    def log(self):
        """Log the error details to CloudWatch"""
        error_info = {
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details
        }
        logger.error(f"Recording Processing Error: {json.dumps(error_info)}")
        if self.details and isinstance(self.details, Exception):
            logger.error(f"Exception traceback: {traceback.format_exc()}")

def get_secret(secret_name: str):
    region_name = os.environ.get('REGION', 'us-east-1')

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise ErrorHandler(
            message=f"Failed to retrieve secret {secret_name}",
            error_type="SecretsManagerError",
            details=str(e)
        )

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def parse_datetime(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a date string from Zoom API to a timezone-aware datetime object
    compatible with PostgreSQL TIMESTAMP WITH TIME ZONE.

    :param date_str: ISO 8601 date string ('2025-01-01T01:20:50Z')
    :return Timezone-awre datetime object or None if date_str is None
    """
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        return dt
    except ValueError as e:
        logger.error(f"Error parsing date {date_str}: {e}")
        return None


def sanitize_name(name: str) -> str:
    """Replaces invalid characters with underscores and limits length for safety."""
    if not name:
        return 'unknown'
    # Remove or replace invalid chars (e.g., for S3/FS/DB)
    sanitized = re.sub(r'[\/:*?"<>|\\]', '_', name)
    return sanitized[:255]


def upload_to_dropbox_and_get_link(file_content: bytes, file_path: str) -> str:
    """Uploads bytes content to Dropbox and returns a shareable link."""
    access_token = os.environ.get('DROPBOX_ACCESS_TOKEN')
    if not access_token:
        raise ValueError("Dropbox access token not set.")
    
    dbx = dropbox.Dropbox(access_token)
    try:
        # Upload the file content
        dbx.files_upload(file_content, f"/{file_path}", mute=True)
        # Create and return shareable link
        shared_link_metadata = dbx.sharing_create_shared_link_with_settings(f"/{file_path}")
        return shared_link_metadata.url.replace('?dl=0', '?dl=1')  # Make it direct download if preferred
    except ApiError as e:
        logger.error(f"Error with {file_path}")
        raise ErrorHandler(
            message=f"Dropbox upload failed for {file_path}",
            error_type="DropboxError",
            details=str(e)
        )


def insert_to_rds(metadata: Dict):
    """Inserts metadata into RDS PostgreSQL staging table."""
    conn = None
    global _secret_cache
    try:
        if not _secret_cache:
            secret_name = os.environ.get('RDS_SECRET_NAME')
            _secret_cache = get_secret(secret_name)

        # Extract credentials (adjust keys if your secret format differs)
        rds_host = _secret_cache.get('host')
        rds_port = _secret_cache.get('port', 5432)
        rds_db = _secret_cache.get('dbname')
        rds_user = _secret_cache.get('username')
        rds_password = _secret_cache.get('password')

        if not all([rds_host, rds_db, rds_user, rds_password]):
            raise ErrorHandler(
                message="Incomplete RDS credentials in Secrets Manager.",
                error_type="CredentialError"
            )

        # Connect using environment variables
        conn = psycopg2.connect(
            host=rds_host,
            port=rds_port,
            database=rds_db,
            user=rds_user,
            password=rds_password
        )
        cur = conn.cursor()

        logger.info(f"Inserting recording metadata for ID: {metadata.get('id')}, Meeting UUID: {metadata.get('meeting_uuid')}")

        cur.execute("""
            INSERT INTO recording_staging(
                id, meeting_uuid, file_type, file_size, file_extension, 
                recording_start, recording_end, recording_type, dropbox_url, file_path
            ) 
            VALUES (
                %(id)s, %(meeting_uuid)s, %(file_type)s, %(file_size)s, %(file_extension)s,
                %(recording_start)s, %(recording_end)s, %(recording_type)s, 
                %(dropbox_url)s, %(file_path)s
            )
            ON CONFLICT (id) DO UPDATE SET
                meeting_uuid = EXCLUDED.meeting_uuid,
                file_type = EXCLUDED.file_type,
                file_size = EXCLUDED.file_size,
                file_extension = EXCLUDED.file_extension,
                recording_start = EXCLUDED.recording_start,
                recording_end = EXCLUDED.recording_end,
                recording_type = EXCLUDED.recording_type,
                dropbox_url = EXCLUDED.dropbox_url,
                file_path = EXCLUDED.file_path
        """, {
            'id': metadata.get('id'),
            'meeting_uuid': metadata.get('meeting_uuid'),
            'file_type': metadata.get('file_type'),
            'file_size': metadata.get('file_size'),
            'file_extension': metadata.get('file_extension'),
            'recording_start': parse_datetime(metadata.get('recording_start')),
            'recording_end': parse_datetime(metadata.get('recording_end')),
            'recording_type': metadata.get('recording_type'),
            'dropbox_url': metadata.get('dropbox_url'),
            'file_path': metadata.get('file_path')
        })
        conn.commit()
        cur.close()
        logger.info(f"Successfully inserted recording metadata for ID: {metadata.get('id')}")
    except Exception as e:
        if conn:
            conn.rollback()

        raise ErrorHandler(
            message=f"RDS insert failed for recording ID: {metadata.get('id', 'unknown')}",
            error_type="DatabaseError",
            details=str(e)
        )
    finally:
        if conn:
            conn.close()