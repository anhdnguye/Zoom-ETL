from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

import os
from datetime import datetime, timedelta
from typing import List, Dict
import logging

import sys
sys.path.insert(1, '/opt/airflow/src')
from zoom.extract import DataExtractor
from db.load import DataLoader

from dotenv import load_dotenv
load_dotenv()

# Connection parameters for DataLoader
connection_params = {
    "host": os.getenv('ZOOM_HOST_NAME'),
    "dbname": os.getenv('ZOOM_DATABASE'),
    "user": os.getenv('ZOOM_USER'),
    "password": os.getenv('ZOOM_PASS'),
    "port": 5432
}

default_args={
    'owner': 'Anh',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 21)
}

@dag('Zoom_ETL', schedule='@once', default_args=default_args,
     catchup=False, tags=['Zoom'], description='Extracting Data from Zoom')
def etl_process():

    @task
    def get_all_users(**kwargs) -> List[str]:
        """Task to get all users IDs."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return extractor.get_all_user_ids()
    
    @task
    def split_user_ids(user_ids: List[str], chunk_size: int = 1000) -> List[List[str]]:
        """Task to split user IDs into chunks."""
        return [user_ids[i:i + chunk_size] for i in range(0, len(user_ids), chunk_size)]
    
    @task
    def process_user_chunk(chunk: List[str]) -> List[Dict]:
        """Task to process a chunk of user IDs and return user info."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return [{"user_id": user_id, "details": extractor.get_user_details(user_id)} 
                for user_id in chunk]

    @task
    def process_meeting_chunk(chunk: List[str], last_run_timestamp: str) -> List[Dict]:
        """Task to process a chunk of user IDs and return meeting info."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        last_run_dt = datetime.fromisoformat(last_run_timestamp)
        return [{"user_id": user_id, "meeting_ids": extractor.get_meetings(user_id, last_run_dt)} 
                for user_id in chunk]
    
    @task
    def flatten_meeting_info(user_meeting: List[List[Dict]]) -> List[Dict]:
        """Task to flatten meeting info for splitting into chunks."""
        flat_meeting_info = []
        for item in user_meeting:
            if isinstance(item, list):
                flat_meeting_info.extend(item)
            else:
                flat_meeting_info.append(item)
        
        return [
            {"user_id": item['user_id'],
            "meeting_id": meeting_} for item in flat_meeting_info for meeting_ in item['meeting_ids'] if meeting_]
    
    @task
    def split_meetings(meeting_infos: List[Dict], chunk_size: int = 1000) -> List[List[Dict]]:
        """Task to split meeting infos into chunks."""
        return [meeting_infos[i:i + chunk_size] for i in range(0, len(meeting_infos), chunk_size)]

    @task
    def process_meeting_details(meeting_infos: List[Dict]) -> List[Dict]:
        """Task to get meeting details and save to metadata directory."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return [{
            "user_id": meeting['user_id'],
            "meeting_id": meeting['meeting_id'],
            "details": extractor.get_meeting_details(meeting['meeting_id'])
        } for meeting in meeting_infos]
    
    @task
    def process_meeting_participants(meeting_infos: List[Dict]) -> List[Dict]:
        """Task to get meeting participants and save to metadata directory."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return [{
            "user_id": meeting['user_id'],
            "meeting_id": meeting['meeting_id'],
            "participants": extractor.get_meeting_participants(meeting['meeting_id'])
        } for meeting in meeting_infos]
    
    @task
    def get_last_run_timestamp() -> str:
        """Task to get the last time that the pipeline ran"""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return extractor.get_last_run_timestamp()
    
    @task
    def set_last_run_timestamp() -> str:
        """Task to set the current time"""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return extractor.set_last_run_timestamp()
    
    @task
    def load_users(user_infos: List[List[Dict]]) -> None:
        """Task to load user data into the database."""
        loader = DataLoader(connection_params)
        with loader:
            # Flatten if nested and ensure each item is a dict
            flat_user_infos = []
            for item in user_infos:
                if isinstance(item, list):
                    flat_user_infos.extend(item)
                else:
                    flat_user_infos.append(item)
            users = [user_info["details"] for user_info in flat_user_infos if isinstance(user_info, dict)]
            if not users:
                logging.warning(f"No valid user details found in {flat_user_infos}")
                return []
            loader.load_users(users)

    @task
    def load_meetings(meeting_details: List[List[Dict]]) -> None:
        """Task to load meeting data into the database."""
        loader = DataLoader(connection_params)
        with loader:
            flat_meeting_infos = []
            for item in meeting_details:
                if isinstance(item, list):
                    flat_meeting_infos.extend(item)
                else:
                    flat_meeting_infos.append(item)
            meetings = [meeting["details"] for meeting in flat_meeting_infos if isinstance(meeting, dict)]
            if not meetings:
                logging.warning(f"No valid meeting details found in {flat_meeting_infos}")
                return []
            loader.load_meetings(meetings)

    @task
    def load_participants(meeting_participants: List[List[Dict]]) -> None:
        """Task to load participant data into the database."""
        loader = DataLoader(connection_params)
        with loader:
            flat_participants = []
            for item in meeting_participants:
                if isinstance(item, list):
                    flat_participants.extend(item)
                else:
                    flat_participants.append(item)

            participants = []
            for meeting_participants in flat_participants:
                meeting_id = meeting_participants["meeting_id"]
                for participant in meeting_participants["participants"]:
                    participant["meeting_uuid"] = meeting_id
                    participants.append(participant)
            loader.load_participants(participants)

    @task
    def merge_recordings() -> None:
        """Task to merge recordings from staging table to main table."""
        loader = DataLoader(connection_params)
        with loader:
            loader.merge_recordings()
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Get last run timestamp
    last_run = get_last_run_timestamp()

    # Get all users
    user_ids = get_all_users()

    # Split user IDs into chunks
    user_id_chunks = split_user_ids(user_ids, chunk_size=1000)

    # Process user information for each chunk -> List[List[Dict]]
    with TaskGroup(group_id='process_user_info') as user_info_group:
        user_infos = process_user_chunk.expand(chunk=user_id_chunks)

    # Process user meetings for each chunk -> List[List[Dict]]
    with TaskGroup(group_id='process_user_meetings') as user_meetings_group:
        user_meetings = process_meeting_chunk.partial(last_run_timestamp=last_run).expand(chunk=user_id_chunks)

    # Generate meeting id and user id for each user meeting
    meeting_data = flatten_meeting_info(user_meeting=user_meetings)

    # Split meeting IDs into chunks
    meeting_chunks = split_meetings(meeting_data, chunk_size=200)

    # Process meeting details and participants using dynamic task mapping
    with TaskGroup(group_id='meeting_details') as meeting_detail_group:
        meeting_details_tasks = process_meeting_details.expand(meeting_infos=meeting_chunks)
    
    with TaskGroup(group_id='meeting_participants') as meeting_participant_group:
        meeting_participants_tasks = process_meeting_participants.expand(meeting_infos=meeting_chunks)
    
    # Load data into the database
    load_users_task = load_users(user_infos)
    load_meetings_task = load_meetings(meeting_details_tasks)
    load_participants_task = load_participants(meeting_participants_tasks)

    # Merge recordings from staging table to recording table
    merge_recordings_task = merge_recordings()

    # Set last run timestamp after all processing is complete
    set_last_run = set_last_run_timestamp()

    # Define task dependencies
    start >> last_run >> user_ids
    user_ids >> user_id_chunks
    user_id_chunks >> user_info_group
    [user_id_chunks, last_run] >> user_meetings_group
    user_meetings_group >> meeting_data >> meeting_chunks
    meeting_chunks >> [meeting_detail_group, meeting_participant_group]

    # Load data after extraction
    user_infos >> load_users_task
    meeting_detail_group >> load_meetings_task
    [meeting_participant_group, load_meetings_task] >> load_participants_task
    
    # Final dependency chain
    [load_users_task, load_participants_task] >> merge_recordings_task >> set_last_run >> end

etl_process()