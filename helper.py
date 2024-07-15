import os
import subprocess
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaInMemoryUpload
from google.oauth2.credentials import Credentials
import os
import boto3
from botocore.exceptions import NoCredentialsError
from retry import retry
import time
from googleapiclient.errors import ResumableUploadError
import ssl
import re

def wait_for_s3_object(s3, bucket, key, local_filepath):
    """
    Wait for an S3 object to exist and have the same size as the local file.
    """
    local_filesize = os.path.getsize(local_filepath)
    while True:
        try:
            response = s3.head_object(Bucket=bucket, Key=key)
            if 'ContentLength' in response and response['ContentLength'] == local_filesize:
                return True
        except Exception as e:
            pass
        ("waited")
        time.sleep(1)
        
def concatenate_videos_aws(intro_resized_filename, main_filename, outro_resized_filename, output_filename, service, stitch_folder):
    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    BUCKET_NAME = 'li-general-task'
    S3_INPUT_PREFIX = 'input_videos/'
    S3_OUTPUT_PREFIX = 'output_videos/'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Create a unique identifier based on the main file name
    unique_id = main_filename.rsplit('_', 1)[0]


    # Initialize boto3 client for AWS MediaConvert
    client = boto3.client('mediaconvert',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url='https://wa11sy9gb.mediaconvert.us-east-2.amazonaws.com'
    )

    def wait_for_job_completion(client, job_id):
        while True:
            response = client.get_job(Id=job_id)
            status = response['Job']['Status']
            if status in ['COMPLETE', 'ERROR', 'CANCELED']:
                return status
            time.sleep(5)

    # Define the input files and their roles
    inputs = [
        {
            'FileInput': f's3://{BUCKET_NAME}/{S3_INPUT_PREFIX}{intro_resized_filename}',
            'AudioSelectors': {
                'Audio Selector 1': {
                    'DefaultSelection': 'DEFAULT',
                    'SelectorType': 'TRACK',
                    'Offset': 0,
                    'ProgramSelection': 1,
                }
            }
        },
        {
            'FileInput': f's3://{BUCKET_NAME}/{S3_INPUT_PREFIX}{main_filename}',
            'AudioSelectors': {
                'Audio Selector 2': {
                    'DefaultSelection': 'DEFAULT',
                    'SelectorType': 'TRACK',
                    'Offset': 0,
                    'ProgramSelection': 1,
                }
            }
        },
        {
            'FileInput': f's3://{BUCKET_NAME}/{S3_INPUT_PREFIX}{outro_resized_filename}',
            'AudioSelectors': {
                'Audio Selector 3': {
                    'DefaultSelection': 'DEFAULT',
                    'SelectorType': 'TRACK',
                    'Offset': 0,
                    'ProgramSelection': 1,
                }
            }
        }
    ]

    output = {
        'Extension': 'mp4',
        'ContainerSettings': {
            'Container': 'MP4',
            'Mp4Settings': {
                'CslgAtom': 'INCLUDE',
                'FreeSpaceBox': 'EXCLUDE',
                'MoovPlacement': 'PROGRESSIVE_DOWNLOAD'
            }
        },
        'VideoDescription': {
            'CodecSettings': {
                'Codec': 'H_264',
                'H264Settings': {
                    'CodecProfile': 'MAIN',
                    'CodecLevel': 'AUTO',
                    'RateControlMode': 'QVBR',
                    'MaxBitrate': 5000000  # Example value, adjust as needed
                }
            }
        },
        'AudioDescriptions': [{
            'AudioSourceName': 'Audio Selector 1', # Specify the name of the audio selector
            'CodecSettings': {
                'Codec': 'AAC',
                'AacSettings': {
                    'AudioDescriptionBroadcasterMix': 'NORMAL',
                    'RateControlMode': 'CBR',
                    'CodecProfile': 'LC',
                    'CodingMode': 'CODING_MODE_2_0',
                    'SampleRate': 48000,
                    'Bitrate': 96000
                }
            }
        }]
    }

    # Create the job settings
    job_settings = {
        'Inputs': inputs,
        'OutputGroups': [{
            'Name': 'File Group',
            'Outputs': [output],
            'OutputGroupSettings': {
                'Type': 'FILE_GROUP_SETTINGS',
                'FileGroupSettings': {
                    'Destination': f's3://{BUCKET_NAME}/{S3_OUTPUT_PREFIX}{output_filename.rsplit(".", 1)[0]}'
                }
            }
        }]
    }


    try:
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Submit the job
                response = client.create_job(Role='arn:aws:iam::339713096623:role/MediaConvertRole', Settings=job_settings)
                job_id = response['Job']['Id']
                print('Job created:', job_id)

                # Wait for the job to finish
                job_status = wait_for_job_completion(client, job_id)
                if job_status == 'COMPLETE':
                    print('Job completed successfully.')
                    break
                elif job_status == 'ERROR':
                    response = client.get_job(Id=job_id)
                    error_message = response['Job'].get('ErrorMessage', 'No error message provided.')
                    print('Job failed with error:', error_message)
                    retry_count += 1
                    print(f"Retrying job submission... (Attempt {retry_count}/{max_retries})")
                else:
                    print(f'Job failed with status: {job_status}')
                    return
            except Exception as e:
                print('Error:', e)
                retry_count += 1
                print(f"Retrying job submission... (Attempt {retry_count}/{max_retries})")

        if retry_count == max_retries:
            print('Maximum number of retries reached. Job submission failed.')
            return
    
        # Use Google Drive API to upload the video from S3 to Google Drive
        s3_url = f'https://{BUCKET_NAME}.s3.amazonaws.com/{S3_OUTPUT_PREFIX}{output_filename.rsplit(".", 1)[0]}.mp4'
        response = requests.get(s3_url, stream=True)
        upload_video(response.raw, stitch_folder, service, output_filename)

        # Clean up the S3 bucket by deleting the files
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=S3_INPUT_PREFIX + f'{unique_id}_intro.mp4')
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=S3_INPUT_PREFIX + f'{unique_id}_main.mp4')
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=S3_INPUT_PREFIX + f'{unique_id}_outro.mp4')
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=S3_OUTPUT_PREFIX + output_filename.rsplit(".", 1)[0] + '.mp4')
        return


    except Exception as e:
        print('Error:', e)

    return

@retry((ssl.SSLEOFError, ResumableUploadError), tries=5, delay=2, backoff=2)
def download_video(file_id, filename, service):
    try:
        request = service.files().get_media(fileId=file_id)
        with open(filename, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
    except:
        print("retried!")
    
def upload_video(stream, folder_id, service, output_filename):
    file_metadata = {'name': output_filename, 'parents': [folder_id]}
    media = MediaInMemoryUpload(stream.read(), mimetype='video/mp4', resumable=True)
    try:
        return service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    except (ssl.SSLEOFError, ResumableUploadError) as e:
        print(f"{e.__class__.__name__} encountered, retrying...")
        raise

import requests
from io import BytesIO

def process_video(data):
    row_number, row, videos_directory, creds_dict, stitch_folder = data
    creds = Credentials.from_authorized_user_info(creds_dict)
    service = build('drive', 'v3', credentials=creds)

    # Create a unique identifier based on the row name
    unique_id = row['name']

    # Dictionary to hold AWS credentials
    aws_credentials = {}

    # Read AWS details directly from amazon.txt
    with open("amazon.txt", 'r') as file:
        for line in file:
            # Clean up line to remove potential hidden characters like '\r'
            line = line.strip().replace('\r', '')
            if ' = ' in line:
                key, value = line.split(' = ')
                aws_credentials[key] = value.strip("'")

    # Assign variables from the dictionary if they exist
    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    BUCKET_NAME = 'li-general-task'
    S3_INPUT_PREFIX = 'input_videos/'
    S3_OUTPUT_PREFIX = 'output_videos/'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Check if the main video is a local file or a Google Drive URL
    if row['main'].startswith('http'):
        # It's a Google Drive URL
        main_file_id = row['main'].split("/file/d/")[1].split("/view")[0]
        stream_video_to_s3(service, main_file_id, f'{unique_id}_main.mp4', s3, BUCKET_NAME, S3_INPUT_PREFIX)
    else:
        # It's a local file
        s3.upload_file(row['main'], BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_main.mp4')

    # Check if the intro video is a local file or a Google Drive URL
    if row['intro'].startswith('http'):
        # It's a Google Drive URL
        intro_file_id = row['intro'].split("/file/d/")[1].split("/view")[0]
        stream_video_to_s3(service, intro_file_id, f'{unique_id}_intro.mp4', s3, BUCKET_NAME, S3_INPUT_PREFIX)
    else:
        # It's a local file
        s3.upload_file(row['intro'], BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_intro.mp4')

    # Concatenate video clips
    output_filename = f"{row['name']}_final.mp4"
    concatenate_videos_aws(f'{unique_id}_intro.mp4', f'{unique_id}_main.mp4', "outro.mp4", output_filename, service, stitch_folder)

    return row['name']

import io
import logging
import time
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

import os
import time
import logging
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

def stream_video_to_s3(service, file_id, s3_filename, s3_client, bucket_name, s3_prefix):
    start_time = time.time()
    try:
        # Get the file metadata to get the file size
        file_metadata = service.files().get(fileId=file_id, fields="size,name").execute()
        file_size = int(file_metadata['size'])
        file_name = file_metadata['name']
        print(f"Starting download of file: {file_name} (ID: {file_id}), Size: {file_size / (1024*1024):.2f} MB")

        # Set up the download request
        request = service.files().get_media(fileId=file_id)

        # Create a BytesIO object to store the downloaded chunks
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request, chunksize=1000*1024*1024)

        # Download the file in chunks
        done = False
        last_progress = 0
        stall_count = 0
        while not done:
            try:
                status, done = downloader.next_chunk(num_retries=5)
                if status:
                    progress = int(status.progress() * 100)
                    if progress > last_progress:
                        print(f"Download {progress}% complete.")
                        last_progress = progress
                        stall_count = 0
                    else:
                        stall_count += 1
                        if stall_count > 10:  # If progress stalls for 10 iterations
                            print(f"Download seems to be stalled at {progress}%")
                            stall_count = 0
            except HttpError as e:
                logging.error(f"HTTP error during download: {e}")
                if e.resp.status in [403, 500, 503]:  # Retry on these errors
                    logging.info("Retrying download...")
                    time.sleep(5)
                else:
                    raise

        # Reset buffer position to the beginning
        buffer.seek(0)

        # Upload to S3
        logging.info(f"Starting upload to S3: {s3_prefix + s3_filename}")
        s3_client.upload_fileobj(buffer, bucket_name, s3_prefix + s3_filename)

        end_time = time.time()
        logging.info(f"File {file_id} successfully streamed to S3 as {s3_filename}")
        logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Error streaming file {file_id} to S3: {str(e)}")
        raise

    finally:
        buffer.close()