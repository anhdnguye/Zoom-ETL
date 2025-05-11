import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pytz
from typing import List, Dict, Any, Optional
import logging

class DataLoader:
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.logger = logging.getLogger(__name__)
        self.batch_size = 1000  # Configurable batch size
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            self.logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _execute_batch(self, query: str, data: List[tuple], table_name: str):
        """Execute batch insert/update operation."""
        try:
            execute_values(self.cursor, query, data, page_size=self.batch_size)
            self.conn.commit()
            self.logger.info(f"Successfully loaded batch of {len(data)} records to {table_name}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error loading batch to {table_name}: {e}")
            raise

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
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

    def load_users(self, users: List[Dict[str, Any]]):
        """Load user data in batches."""
        if not users:
            self.logger.info("No users to load")
            return

        query = """
            INSERT INTO "user" (
                id, email, first_name, last_name, dept, role_name,
                created_at, last_login_time, group_names
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                email = EXCLUDED.email,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                dept = EXCLUDED.dept,
                role_name = EXCLUDED.role_name,
                created_at = EXCLUDED.created_at,
                last_login_time = EXCLUDED.last_login_time,
                group_names = EXCLUDED.group_names
        """

        for i in range(0, len(users), self.batch_size):
            batch = users[i:i + self.batch_size]
            data = [
                (
                    user.get('id'),
                    user.get('email'),
                    user.get('first_name'),
                    user.get('last_name'),
                    user.get('dept'),
                    user.get('role_name'),
                    self._parse_datetime(user.get('created_at')),
                    self._parse_datetime(user.get('last_login_time')),
                    user.get('group_names', [])
                )
                for user in batch
            ]
            self._execute_batch(query, data, "user")

    def load_meetings(self, meetings: List[Dict[str, Any]]):
        """Load meeting data in batches."""
        if not meetings:
            self.logger.info("No meetings to load")
            return

        query = """
            INSERT INTO meeting (
                id, uuid, host_id, topic, start_time, end_time,
                duration, participants_count, type, has_recording
            )
            VALUES %s
            ON CONFLICT (uuid) DO UPDATE SET
                id = EXCLUDED.id,
                host_id = EXCLUDED.host_id,
                topic = EXCLUDED.topic,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                duration = EXCLUDED.duration,
                participants_count = EXCLUDED.participants_count,
                type = EXCLUDED.type,
                has_recording = EXCLUDED.has_recording
        """

        for i in range(0, len(meetings), self.batch_size):
            batch = meetings[i:i + self.batch_size]
            data = [
                (
                    meeting.get('id'),
                    meeting.get('uuid'),
                    meeting.get('host_id'),
                    meeting.get('topic'),
                    meeting.get('start_time'),
                    meeting.get('end_time'),
                    meeting.get('duration'),
                    meeting.get('participants_count'),
                    meeting.get('type'),
                    meeting.get('has_recording', False)
                )
                for meeting in batch
            ]
            self._execute_batch(query, data, "meeting")

    def load_participants(self, participants: List[Dict[str, Any]]):
        """Load participant data in batches."""
        if not participants:
            self.logger.info("No participants to load")
            return

        query = """
            INSERT INTO participant (
                meeting_uuid, user_id, name, email, join_time,
                leave_time, duration, internal_user
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                meeting_uuid = EXCLUDED.meeting_uuid,
                user_id = EXCLUDED.user_id,
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                join_time = EXCLUDED.join_time,
                leave_time = EXCLUDED.leave_time,
                duration = EXCLUDED.duration,
                internal_user = EXCLUDED.internal_user
        """

        for i in range(0, len(participants), self.batch_size):
            batch = participants[i:i + self.batch_size]
            data = [
                (
                    participant.get('meeting_uuid'),
                    participant.get('user_id'),
                    participant.get('name'),
                    participant.get('email'),
                    participant.get('join_time'),
                    participant.get('leave_time'),
                    participant.get('duration'),
                    participant.get('internal_user', False)
                )
                for participant in batch
            ]
            self._execute_batch(query, data, "participant")