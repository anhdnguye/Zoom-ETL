import os
import re
import dropbox
from dropbox.exceptions import ApiError
from typing import Dict, Optional
from datetime import datetime
import pytz
import psycopg2

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
        print(f"Error parsing date {date_str}: {e}")
        return None


def sanitize_name(name: str) -> str:
    """Replaces invalid characters with underscores and limits length for safety."""
    if not name:
        return 'unknown'
    # Remove or replace invalid chars (e.g., for S3/FS/DB)
    sanitized = re.sub(r'[\/:*?"<>|\\]', '_', name)
    return sanitized[:255]


def upload_to_dropbox_and_get_link(file_content, file_path: str):
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
        raise RuntimeError(f"Dropbox upload failed: {str(e)}")


def insert_to_rds(metadata: Dict):
    """Inserts metadata into RDS PostgreSQL staging table."""
    conn = None
    try:
        # Connect using environment variables
        conn = psycopg2.connect(
            host=os.environ.get('RDS_HOST'),
            port=os.environ.get('RDS_PORT', 5432),
            database=os.environ.get('RDS_DB'),
            user=os.environ.get('RDS_USER'),
            password=os.environ.get('RDS_PASSWORD')
        )
        cur = conn.cursor()

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
    except Exception as e:
        if conn:
            conn.rollback()
        raise RuntimeError(f"RDS insert failed: {str(e)}")
    finally:
        if conn:
            conn.close()