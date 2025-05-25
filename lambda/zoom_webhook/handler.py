import json
import boto3
import base64
import os
from datetime import datetime
import requests

from utils import *

s3_client = boto3.client('s3')

S3_BUCKET = os.environ.get('S3_BUCKET_NAME', 'your-default-bucket')

def select_preferred_recordings(zoom_recordings):
    accepted_recording = [
        ['shared_screen_with_gallery_view(CC)', 'shared_screen_with_gallery_view', 'shared_screen_with_speaker_view', 'gallery_view', 'active_speaker', 'audio_only'],
        ['audio_transcript'],
        ['chat_file'],
        ['poll']
    ]
    # Convert zoom recordings to a dict for fast lookup
    file_lookup = {rec['file_type']: rec for rec in zoom_recordings}

    selected = []

    for category in accepted_recording:
        for preferred_type in category:
            if preferred_type in file_lookup:
                selected.append(file_lookup[preferred_type])
                break  # Only take the first available in this category

    return selected

def lambda_handler(event, context):
    try:
        print("Received event:", json.dumps(event))

        # Zoom Webhook data (body is stringified JSON)
        body = json.loads(event['body'])
        recording_data = body.get('payload', {}).get('object', {})

        meeting_uuid = recording_data.get('uuid')
        topic = sanitize_name(recording_data.get('topic', 'unknown'))
        host_email = recording_data.get('host_email', 'unknown')
        start_time = recording_data.get('start_time')
        recording_files = recording_data.get('recording_files', [])

        selected_files = select_preferred_recordings(recording_files)

        for metadata in selected_files:
            metadata.update(meeting_uuid=meeting_uuid)

            if not metadata.get('download_url'):
                continue
            
            file_type = metadata.get('recording_type')
            file_ext = metadata.get('file_extension')
            # Example: Save metadata to S3 (you can also use Dropbox SDK)
            s3_key = f"recordings/{host_email}/{topic}/{start_time}/{file_type}.{file_ext}"

            response = requests.get(metadata.get('download_url'), stream=True)
            response.raise_for_status()

            # Read content into memory (for small files) or use temp file for larger ones
            content = io.BytesIO()
            for chunk in response.iter_content(chunk_size=512 * 1024):
                content.write(chunk)
            content.seek(0)

            s3_client.upload_fileobj(
                Fileobj=content,
                Bucket=S3_BUCKET,
                Key=s3_key
            )
            s3_link = f"s3://{S3_BUCKET}/{s3_key}"
            metadata.update(file_path=s3_link)

            # Reset content position for Dropbox upload
            content.seek(0)

            try:
                dropbox_link = upload_to_dropbox_and_get_link(response.content, s3_key)
                metadata.update(dropbox_url=dropbox_link)
            except Exception as e:
                print(f"Dropbox upload failed: {str(e)}")
                metadata.update(dropbox_url=None)
            
            insert_to_rds(metadata)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Recording processed successfully"})
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
