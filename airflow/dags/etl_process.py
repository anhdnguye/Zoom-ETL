from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

import os
from datetime import datetime, timedelta
from typing import List, Dict

import sys
sys.path.insert(1, '/opt/airflow/scripts')
from extract import DataExtractor
from load import DataLoader

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
    'email': ['test@test.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 21)
    }

@dag('ETL', schedule='@once', default_args=default_args,
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
    def generate_meeting_info(user_meeting: Dict) -> List[Dict]:
        """Task to generate meeting info for dynamic mapping."""
        meeting_ids = user_meeting['meeting_ids']
        user_id = user_meeting['user_id']
        if not meeting_ids:
            return []
        return [{"user_id": user_id, "meeting_id": meeting_id} for meeting_id in meeting_ids]
    
    @task
    def get_meeting_details(meeting_info: Dict) -> Dict:
        """Task to get meeting details and save to metadata directory."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        meeting_id = meeting_info['meeting_id']
        user_id = meeting_info['user_id']
        meeting_details = extractor.get_meeting_details(meeting_id)
        return {
            "user_id": user_id,
            "meeting_id": meeting_id,
            "details": meeting_details
        }
    @task
    def get_meeting_participants(meeting_info: Dict) -> Dict:
        """Task to get meeting participants and save to metadata directory."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        meeting_id = meeting_info['meeting_id']
        user_id = meeting_info['user_id']
        participants = extractor.get_meeting_participants(meeting_id)
        return {
            "user_id": user_id,
            "meeting_id": meeting_id,
            "participants": participants
        }
    
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
    def load_users(user_infos: List[Dict]) -> None:
        """Task to load user data into the database."""
        loader = DataLoader(connection_params)
        with loader:
            users = [user_info["details"] for user_info in user_infos]
            loader.load_users(users)

    @task
    def load_meetings(meeting_details_tasks: List[Dict]) -> None:
        """Task to load meeting data into the database."""
        loader = DataLoader(connection_params)
        with loader:
            meetings = [meeting["details"] for meeting in meeting_details_tasks]
            loader.load_meetings(meetings)

    @task
    def load_participants(meeting_participants_tasks: List[Dict]) -> None:
        """Task to load participant data into the database."""
        loader = DataLoader(connection_params)
        with loader:
            participants = []
            for meeting_participants in meeting_participants_tasks:
                meeting_id = meeting_participants["meeting_id"]
                for participant in meeting_participants["participants"]:
                    participant["meeting_uuid"] = meeting_id
                    participants.append(participant)
            loader.load_participants(participants)
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Get last run timestamp
    last_run = get_last_run_timestamp()

    # Get all users
    user_ids = get_all_users()

    # Split user IDs into chunks
    user_id_chunks = split_user_ids(user_ids, chunk_size=1000)

    # Process user information for each chunk
    with TaskGroup(group_id='process_user_info') as user_info_group:
        user_infos = process_user_chunk.expand(chunk=user_id_chunks)

    # Process user meetings for each chunk
    with TaskGroup(group_id='process_user_meetings') as user_meetings_group:
        user_meetings = process_meeting_chunk.partial(last_run_timestamp=last_run).expand(chunk=user_id_chunks)

    # Generate meeting id and user id for each user meeting
    meeting_infos = generate_meeting_info.expand(user_meeting=user_meetings)

    # Process meeting details and participants using dynamic task mapping
    meeting_details_tasks = get_meeting_details.expand(meeting_info=meeting_infos)
    meeting_participants_tasks = get_meeting_participants.expand(meeting_info=meeting_infos)
    
    # Load data into the database
    load_users_task = load_users(user_infos)
    load_meetings_task = load_meetings(meeting_details_tasks)
    load_participants_task = load_participants(meeting_participants_tasks)

    # Set last run timestamp after all processing is complete
    set_last_run = set_last_run_timestamp()

    # Define task dependencies
    start >> last_run >> user_ids
    user_ids >> user_id_chunks
    user_id_chunks >> user_info_group
    [user_id_chunks, last_run] >> user_meetings_group
    user_meetings_group >> meeting_infos
    meeting_infos >> [meeting_details_tasks, meeting_participants_tasks]

    # Load data after extraction
    user_infos >> load_users_task
    meeting_details_tasks >> load_meetings_task
    meeting_participants_tasks >> load_participants_task
    
    # Final dependency chain
    [load_users_task, load_meetings_task, load_participants_task] >> set_last_run >> end

etl_process()