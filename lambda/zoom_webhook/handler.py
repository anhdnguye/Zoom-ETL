import json
import boto3
import logging
import os
import requests

from utils import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_resource = boto3.resource('s3')
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
        logger.info("Received Zoom event:", json.dumps(event))

        # Zoom Webhook data (body is stringified JSON)
        body = json.loads(event['body'])
        recording_data = body.get('payload', {}).get('object', {})

        required_fields = ['uuid', 'topic', 'host_email', 'start_time', 'recording_files']
        missing_fields = [field for field in required_fields if field not in recording_data]
        if missing_fields:
            raise ErrorHandler(
                message=f"Missing required fields in payload: {', '.join(missing_fields)}",
                error_type="ValidationError"
            )
        meeting_uuid = recording_data.get('uuid')
        topic = sanitize_name(recording_data.get('topic', 'unknown'))
        host_email = recording_data.get('host_email', 'unknown')
        start_time = recording_data.get('start_time')
        recording_files = recording_data.get('recording_files', [])

        selected_files = select_preferred_recordings(recording_files)

        for metadata in selected_files:
            metadata['meeting_uuid'] = meeting_uuid

            if not metadata.get('download_url'):
                logger.warning(f"Skipping file without download_url: {metadata.get('id')}")
                continue
            
            file_type = metadata.get('recording_type')
            file_ext = metadata.get('file_extension')
            s3_key = f"recordings/{host_email}/{topic}/{start_time}/{file_type}.{file_ext}"

            try:
                with requests.get(metadata['download_url'], stream=True, timeout=30) as response:
                    response.raise_for_status()
                    s3_resource.Bucket(S3_BUCKET).upload_fileobj(response.raw, s3_key)
                    s3_link = f"s3://{S3_BUCKET}/{s3_key}"
                    metadata['file_path'] = s3_link

                    response.raw.seek(0)

                    try:
                        dropbox_link = upload_to_dropbox_and_get_link(response.raw, s3_key)
                        metadata['dropbox_url'] = dropbox_link
                    except ErrorHandler as e:
                        logger.error("Dropbox upload failed: %s", str(e), exc_info=True)
                        metadata['dropbox_url'] = None

                insert_to_rds(metadata)

            except requests.exceptions.RequestException as e:
                raise ErrorHandler(
                    message=f"File download/upload failed for {s3_key}: {str(e)}",
                    error_type="DownloadUploadError"
                )
            except Exception as e:
                logger.error(f"Error processing file {s3_key}, {str(e)}", exc_info=True)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Recording processed successfully"})
        }

    except ErrorHandler as e:
        logger.error(f"Webhook processing error: {str(e)}", exc_info=True)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"})
        }
