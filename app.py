import streamlit as st
import pandas as pd
import shotstack_sdk as shotstack
from shotstack_sdk.api import edit_api
from shotstack_sdk.model.template_render import TemplateRender
from shotstack_sdk.model.merge_field import MergeField
import requests
import os
import shutil
import time
import zipfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
import sys
import json
import boto3
from PIL import Image
from io import BytesIO
import matplotlib.pyplot as plt
import numpy as np
from fpdf import FPDF
import uuid
from helper import process_video
from intros import intro_process_video
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os
import subprocess
import re
import moviepy.editor as mp
from moviepy.editor import VideoFileClip, AudioFileClip, concatenate_videoclips
import subprocess
import traceback
from googleapiclient.errors import HttpError
from botocore.exceptions import ClientError

st.set_page_config(
    page_title='VideoEditor',
    page_icon='📹'
) 
hide_streamlit_style = """ <style> #MainMenu {visibility: hidden;} footer {visibility: hidden;} </style> """ 
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

def extract_id_from_url(url):
    match = re.search(r'(?<=folders/)[a-zA-Z0-9_-]+', url)
    if match:
        return match.group(0)
    match = re.search(r'(?<=spreadsheets/d/)[a-zA-Z0-9_-]+', url)
    if match:
        return match.group(0)
    return None

def reset_s3():
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

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Delete objects within subdirectories in the bucket 'li-general-task'
    subdirs = ['input_videos/', 'output_videos/', 'images/']
    for subdir in subdirs:
        objects = s3.list_objects_v2(Bucket='li-general-task', Prefix=subdir)
        for obj in objects.get('Contents', []):
            if obj['Key'] != 'input_videos/outro.mp4':
                s3.delete_object(Bucket='li-general-task', Key=obj['Key'])
                
        # Add a placeholder object to represent the "directory"
        s3.put_object(Bucket='li-general-task', Key=subdir)

try:
    
    if 'begin_auth' not in st.session_state:
        reset_s3()
        st.session_state['creds'] = ""
        st.session_state['begin_auth'] = False
        st.session_state['final_auth'] = False

except:
    pass

# Title of the app
st.title("LI Video Editor")
st.caption("By Giacomo Pugliese")

with st.expander("Click to view full directions for this site"):
    st.subheader("Google Authentication")
    st.write("- Click 'Authenticate Google Account', and then on the generated link.")
    st.write("- Follow the steps of Google login until you get to the final page.")
    st.write("- Click on 'Finalize Authentication' to proceed to rest of website.")
    st.subheader("Video Intro Generator")
    st.write("- Enter the intended output Google drive folder link, as well as the program name of the students.")
    st.write("- If a solo intern video, upload a csv with columns PRECISELY titled 'name', 'school', 'location', and 'class'.")
    st.write("- If a group video, upload a csv with columns PRECISELY titled 'name1', 'name2', 'name3'.... (max 7 interns).")
    st.write("- Click 'Process Solo Videos' or 'Process Team Videos' (depending on your intended output format) to begin intro video renderings and view them in your destination Google drive folder.")
    st.subheader("Video Stitcher")
    st.write("- Enter the intended output Google drive folder link")
    st.write("- Upload a csv with columns PRECISELY titled 'name', 'intro', and 'main' (reffering to the intro and main video share links).")
    st.write("- Click 'Stitch Videos' to begin video stitching and view them in your destination Google drive folder.")
    st.subheader("Automatic Youtube Uploader")
    st.write("- Upload a csv with columns PRECISELY titled 'title' and 'video' (the video column should have a Google drive share link).")
    st.write("- Click 'Upload videos to youtube' and view them in your youtube channel.")
    st.subheader("Video Stitcher")
    st.write("- Enter the name of the intended input and output folder within the 'video-stitch' folder located in the baillymarshall@lichange.org aws account.")
    st.write("- Ensure that all videos within the input folder are in groups with format [name]_intro.mp4, [name]_main.mp4., and [name]_judge.mp4 (optional).")
    st.write("- Click 'Start Concatenation' to begin video stitching and view them in your destination s3 output folder wtihin the video-stich folder.")
    st.subheader("Presentation Downloader Tool")
    st.write("- NOTE: This tool is only intended for use within a local environment (not on the streamlit cloud).")
    st.write("- Enter a csv file with columns PRECISELY named 'name', 'intro', 'main', and 'judge' (optional), each containing a column of google drive video links.")
    st.write("- Click 'Download Videos' to begin video downloads locally that are in the correct naming convention for use of the S3 Video Stitcher. Find them in the local directory within folders called 'intro_videos' and 'main_videos'.")

st.header("Google Authentication")

try:
    if st.button("Authenticate Google Account"):
        st.session_state['begin_auth'] = True
        # Request OAuth URL from the FastAPI backend
        response = requests.get(f"{'https://leadership-initiatives-0c372bea22f2.herokuapp.com'}/auth?user_id={'intros'}")
        if response.status_code == 200:
            # Get the authorization URL from the response
            auth_url = response.json().get('authorization_url')
            st.markdown(f"""
                <a href="{auth_url}" target="_blank" style="color: #8cdaf2;">
                    Click to continue to authentication page (before finalizing)


                </a>
                """, unsafe_allow_html=True)
            st.text("\n\n\n")
            # Redirect user to the OAuth URL
            # nav_to(auth_url)

    if st.session_state['begin_auth']:    
        if st.button("Finalize Google Authentication"):
            with st.spinner("Finalizing authentication..."):
                for i in range(6):
                    # Request token from the FastAPI backend
                    response = requests.get(f"{'https://leadership-initiatives-0c372bea22f2.herokuapp.com'}/token/{'intros'}")
                    if response.status_code == 200:
                        st.session_state['creds'] = response.json().get('creds')
                        print(st.session_state['creds'])
                        st.success("Google account successfully authenticated!")
                        st.session_state['final_auth'] = True
                        break
                    time.sleep(1)
            if not st.session_state['final_auth']:
                st.error('Experiencing network issues, please refresh page and try again.')
                st.session_state['begin_auth'] = False

except Exception as e:
    pass

st.header("Video Intro Generator")

col1, col2 = st.columns(2)

with col1:
    # Get the ID of the Google Drive folder to upload the videos to
    folder_id = st.text_input("URL of the Google Drive folder to upload the videos to:")

with col2:
    # Text input for the program name
    program = st.text_input("Enter the Program Name:")

# In app.py, replace the file uploader with a text input for the Google Sheet URL
sheet_url = st.text_input("Enter the URL of the Google Sheet:")

# Configure the Shotstack API
configuration = shotstack.Configuration(host = "https://api.shotstack.io/v1")
configuration.api_key['DeveloperKey'] = "PudC3eZKBond8D64AHAE2UNbwcdEvvbjuEr3Sm7b"

solo_video_button = st.button("Process Solo Videos")
team_video_button = st.button("Process Team Videos")

def get_video_info(filename):
    result = subprocess.run(["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", filename],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    return json.loads(result.stdout)

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

def update_progress_report(message):
    est = pytz.timezone('US/Eastern')
    timestamp = datetime.now(est).strftime('%Y-%m-%d %H:%M:%S EST')
    with open('progress_report.txt', 'a') as f:
        print(f"[{timestamp}] {message}\n")

def wait_for_job_completion(mediaconvert, job_id):
    while True:
        try:
            response = mediaconvert.get_job(Id=job_id)
            status = response['Job']['Status']
            
            if status == 'COMPLETE':
                print(f"Job {job_id} completed successfully!")
                return True
            elif status == 'ERROR':
                print(f"Job {job_id} failed. Error message: {response['Job']['ErrorMessage']}")
                return False
            else:
                print(f"Job {job_id} is {status}. Waiting...")
                time.sleep(30)  # Wait for 30 seconds before checking again
        except botocore.exceptions.ClientError as e:
            update_progress_report(f"An error occurred while checking job status: {e}")
            return False
            
if sheet_url is not None and program and team_video_button and st.session_state['final_auth']:
    with st.spinner("Processing videos (may take a few minutes)..."):
        folder_id = extract_id_from_url(folder_id)
        # Load the CSV file into a dataframe
        # Extract the sheet ID from the URL
        # Extract the sheet ID from the URL
        sheet_id = extract_id_from_url(sheet_url)
        st.session_state['creds']['refresh_token'] = st.session_state['creds']['_refresh_token']
        # Use the Google Sheets API to fetch the data
        creds = Credentials.from_authorized_user_info(st.session_state['creds'])
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range='A:Z').execute()
        values = result.get('values', [])
        
        if not values:
            st.error('No data found in the sheet.')
        else:
            # Convert the sheet data to a DataFrame
            headers = values[0]
            data = values[1:]
            
            # Ensure all rows have the same number of columns as the header
            max_cols = len(headers)
            data = [row + [''] * (max_cols - len(row)) for row in data]
            
            try:
                dataframe = pd.DataFrame(data, columns=headers)
            except ValueError as e:
                st.error(f"Error creating DataFrame: {e}")
                st.error("Please ensure your Google Sheet has consistent column headers and data.")
                st.stop()

            # Display the first few rows of the dataframe for verification
            # st.write("First few rows of the data:")
            # st.write(dataframe.head())

        # Create API client
        with shotstack.ApiClient(configuration) as api_client:
            api_instance = edit_api.EditApi(api_client)

            progress_report = st.empty()
            i = 1
            # Loop over the rows of the dataframe
            for index, row in dataframe.iterrows():
                # Create the merge fields for this row
                merge_fields = [
                    MergeField(find="program_name", replace=program),
                    MergeField(find="name1", replace=row.get('name1', '') if pd.notna(row.get('name1', '')) else ''),
                    MergeField(find="name2", replace=row.get('name2', '') if pd.notna(row.get('name2', '')) else ''),
                    MergeField(find="name3", replace=row.get('name3', '') if pd.notna(row.get('name3', '')) else ''),
                    MergeField(find="name4", replace='Class of ' + str(round(row['year'])) if 'year' in row and pd.notna(row['year']) else row.get('name4', '') if pd.notna(row.get('name4', '')) else ''),
                    MergeField(find="name5", replace=row.get('name5', '') if pd.notna(row.get('name5', '')) else ''),
                    MergeField(find="name6", replace=row.get('name6', '') if pd.notna(row.get('name6', '')) else ''),
                    MergeField(find="name7", replace=row.get('name7', '') if pd.notna(row.get('name7', '')) else ''),
                    MergeField(find="name8", replace=row.get('name8', '') if pd.notna(row.get('name8', '')) else ''),
                ]

                # Create the template render object
                template = TemplateRender(
                    id="edf5ffeb-3334-400a-949d-3356c348f1d9",
                    merge=merge_fields
                )


                try:
                    # Post the template render
                    api_response = api_instance.post_template_render(template)

                    # Display the message
                    message = api_response['response']['message']
                    id = api_response['response']['id']
                    print(f"{message}")

                    # Poll the API until the video is ready
                    status = 'queued'
                    while status != 'done':
                        time.sleep(1)
                        try:
                            status_response = api_instance.get_render(id)
                            status = status_response.response.status
                            print(status)
                        except shotstack.exceptions.ApiTypeError as e:
                            print(f"Error in API response: {e}")
                            # Log the error and continue
                            status = 'done'  # Force exit from the loop

                    # Construct the video URL
                    video_url = f"https://cdn.shotstack.io/au/v1/1cr8ajwibd/{id}.mp4"
                    print(video_url)

                    name = row.get('name', row.get('name1', 'unnamed'))
                    main_video_file = f"{name}_main.mp4"
                    time.sleep(5)
                    # Directly write the downloaded content to a file
                    download_file(video_url, main_video_file)

                    # Prepare data for process_video function
                    videos_directory = os.getcwd()
                    CLIENT_SECRET_FILE = 'credentials.json'
                    with open(CLIENT_SECRET_FILE, 'r') as f:
                        client_info = json.load(f)['web']
                    creds_dict = st.session_state['creds']
                    creds_dict['client_id'] = client_info['client_id']
                    creds_dict['client_secret'] = client_info['client_secret']
                    creds_dict['refresh_token'] = creds_dict.get('_refresh_token')

                    # Create a mock row for process_video function
                    mock_row = {
                        'name': name,
                        'intro': os.path.join(videos_directory, 'intro_li.mp4'),
                        'main': main_video_file
                    }


                    process_video_data = (index, mock_row, videos_directory, creds_dict, folder_id, sheet_id)
                    intro_process_video(process_video_data)

                    # Remove temporary main video file
                    os.remove(main_video_file)

                except Exception as e:

                    print(f"Unable to generate or process video for {name}: {e}")
                    traceback.print_exc()

                progress_report.text(f"Video progress: {i}/{len(dataframe)}")
                i+=1

    st.success("Videos successfully generated, processed, and uploaded!")

if sheet_url is not None and program and solo_video_button and st.session_state['final_auth']:
    with st.spinner("Processing videos (may take a few minutes)..."):
        folder_id = extract_id_from_url(folder_id)
        # Load the CSV file into a dataframe
        # Extract the sheet ID from the URL
        sheet_id = extract_id_from_url(sheet_url)
        
        # Use the Google Sheets API to fetch the data
        creds = Credentials.from_authorized_user_info(st.session_state['creds'])
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range='A:Z').execute()
        values = result.get('values', [])
        
        if not values:
            st.error('No data found in the sheet.')
        else:
            # Convert the sheet data to a DataFrame
            headers = values[0]
            data = values[1:]
            
            # Ensure all rows have the same number of columns as the header
            max_cols = len(headers)
            data = [row + [''] * (max_cols - len(row)) for row in data]
            
            try:
                dataframe = pd.DataFrame(data, columns=headers)
            except ValueError as e:
                st.error(f"Error creating DataFrame: {e}")
                st.error("Please ensure your Google Sheet has consistent column headers and data.")
                st.stop()

            # Display the first few rows of the dataframe for verification
            # st.write("First few rows of the data:")
            # st.write(dataframe.head())

        # Create API client
        with shotstack.ApiClient(configuration) as api_client:
            api_instance = edit_api.EditApi(api_client)

            progress_report = st.empty()
            i = 1
            # Loop over the rows of the dataframe
            for index, row in dataframe.iterrows():
                # Create the merge fields for this row
                merge_fields = [
                    MergeField(find="program_name", replace=program),
                    MergeField(find="name", replace=row.get('name', row.get('name1', ''))),
                    MergeField(find="school", replace=row.get('school', row.get('name2', ''))),
                    MergeField(find="location", replace=row.get('location', row.get('name3', ''))),
                    MergeField(find="year", replace='Class of ' + str(round(row['year'])) if 'year' in row else row.get('name4', '')),
                ]

                # Create the template render object
                template = TemplateRender(
                    id = "58dbf2dc-eded-4a71-a629-1bcefe025709",
                    merge = merge_fields
                )

                try:
                    # Post the template render
                    api_response = api_instance.post_template_render(template)

                    # Display the message
                    message = api_response['response']['message']
                    id = api_response['response']['id']
                    print(f"{message}")

                    # Poll the API until the video is ready
                    status = 'queued'
                    while status != 'done':
                        time.sleep(1)
                        try:
                            status_response = api_instance.get_render(id)
                            status = status_response.response.status
                            print(status)
                        except shotstack.exceptions.ApiTypeError as e:
                            print(f"Error in API response: {e}")
                            # Log the error and continue
                            status = 'done'  # Force exit from the loop

                    # Construct the video URL
                    video_url = f"https://cdn.shotstack.io/au/v1/1cr8ajwibd/{id}.mp4"
                    print(video_url)

                    name = row.get('name', row.get('name1', 'unnamed'))
                    main_video_file = f"{name}_main.mp4"
                    time.sleep(5)
                    # Directly write the downloaded content to a file
                    download_file(video_url, main_video_file)

                    # Prepare data for process_video function
                    videos_directory = os.getcwd()
                    CLIENT_SECRET_FILE = 'credentials.json'
                    with open(CLIENT_SECRET_FILE, 'r') as f:
                        client_info = json.load(f)['web']
                    creds_dict = st.session_state['creds']
                    creds_dict['client_id'] = client_info['client_id']
                    creds_dict['client_secret'] = client_info['client_secret']
                    creds_dict['refresh_token'] = creds_dict.get('_refresh_token')

                    # Create a mock row for process_video function
                    mock_row = {
                        'name': name,
                        'intro': os.path.join(videos_directory, 'intro_li.mp4'),
                        'main': main_video_file
                    }


                    process_video_data = (index, mock_row, videos_directory, creds_dict, folder_id)
                    intro_process_video(process_video_data)

                    # Remove temporary main video file
                    os.remove(main_video_file)

                except Exception as e:

                    print(f"Unable to generate or process video for {name}: {e}")
                    traceback.print_exc()

                progress_report.text(f"Video progress: {i}/{len(dataframe)}")
                i+=1

    st.success("Videos successfully generated, processed, and uploaded!")

# Streamlit UI
st.header("Video Stitching Tool")
stitch_folder = st.text_input("URL of the Google Drive folder to upload videos to:")

# File upload widget
stitch_uploaded = st.file_uploader(label="Upload a CSV file of videos", type=['csv'])

# Get user's local "Videos" directory
videos_directory = os.path.join(os.getcwd(), 'Videos')

stitch_button = st.button("Stitch Videos")

if stitch_button and st.session_state['final_auth'] and stitch_folder and stitch_uploaded is not None:
    with st.spinner("Stitching videos (may take a few minutes)..."):
        stitch_folder = extract_id_from_url(stitch_folder)
        df = pd.read_csv(stitch_uploaded)

        # Assuming that 'CLIENT_SECRET_FILE', 'videos_directory', 'stitch_folder', and 'df' are defined elsewhere in your code

        CLIENT_SECRET_FILE = 'credentials.json'
        with open(CLIENT_SECRET_FILE, 'r') as f:
            client_info = json.load(f)['web']
        creds_dict = st.session_state['creds']
        creds_dict['client_id'] = client_info['client_id']
        creds_dict['client_secret'] = client_info['client_secret']
        creds_dict['refresh_token'] = creds_dict.get('_refresh_token')

        arguments = [(index, row, videos_directory, creds_dict, stitch_folder) for index, row in df.iterrows()]

        stitch_progress = st.empty()
        stitch_progress.text(f"Video Progress: 0/{len(df)}")

        i = 0

        for arg in arguments:
            try:
                result = process_video(arg)
                i += 1
                stitch_progress.text(f"Video Progress: {i}/{len(arguments)}")
            except Exception as e:
                # Assuming the 'arg' is a tuple and the first element is the row number
                row_number = arg[0]
                error_message = f"Exception at row {row_number + 2}: {e}"
                error_type = type(e).__name__
                print(f"{error_message}\nError Type: {error_type}\nException Args: {e.args}")
                traceback.print_exc()  # Print the stack trace
    st.success("Videos successfully concatenated and uploaded!")

# Define the required scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly',
          'https://www.googleapis.com/auth/youtube.upload']

def get_authenticated_service():
    flow = flow_from_clientsecrets('credentials.json',
        scope=SCOPES,
        message='Please configure OAuth 2.0')

    # Set the redirect_uri property of the flow object
    flow.redirect_uri = "https://leadership-initiatives-0c372bea22f2.herokuapp.com/callback"

    storage = Storage("%s-oauth2.json" % sys.argv[0])
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)

    return build('youtube', 'v3', credentials=credentials)

def initialize_upload(youtube, video_file, title, description, category_id, tags):
    body = {
        'snippet': {
            'title': title,
            'description': description,
            'tags': tags,
            'categoryId': category_id
        },
        'status': {
            'privacyStatus': 'public'
        }
    }
    media = MediaFileUpload(video_file, mimetype='video/mp4', resumable=True)
    request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        media_body=media
    )
    resumable_upload(request)

def resumable_upload(request):
    response = None
    while response is None:
        status, response = request.next_chunk()
        if response is not None:
            if 'id' in response:
                print(f'Video ID {response["id"]} was successfully uploaded.')
            else:
                print(f'The upload failed with an unexpected response: {response}')

def download_video_from_drive(url, output, creds_dict):
    creds = Credentials.from_authorized_user_info(creds_dict, SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    file_id = url.split('/')[-2]
    video_request = drive_service.files().get_media(fileId=file_id)
    video_data_io = BytesIO()
    downloader = MediaIoBaseDownload(video_data_io, video_request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    with open(output, 'wb') as f:
        f.write(video_data_io.getvalue())

st.header("Automatic Youtube Uploader")
video_uploads = st.file_uploader(label="Upload a CSV of videos", type=['csv'])
if st.button("Upload videos to youtube") and video_uploads:
    youtube = get_authenticated_service()
    df = pd.read_csv(video_uploads)
    CLIENT_SECRET_FILE = 'credentials.json'
    with open(CLIENT_SECRET_FILE, 'r') as f:
        client_info = json.load(f)['web']
    creds_dict = st.session_state['creds']
    creds_dict['client_id'] = client_info['client_id']
    creds_dict['client_secret'] = client_info['client_secret']
    creds_dict['refresh_token'] = creds_dict.get('_refresh_token')
    progress = st.empty()
    i = 1
    progress.text(f"Upload progress: {i}/{len(df)}")
    with st.spinner("Uploading videos (may take a few minutes)..."):
        for index, row in df.iterrows():
            video_url = row['video']
            video_file = f"video_{index}.mp4"
            download_video_from_drive(video_url, video_file, creds_dict) 
            title = row['title']
            description = ""
            category_id = "22"
            tags = []
            try:
                initialize_upload(youtube, video_file, title, description, category_id, tags)
            except HttpError as e:
                st.write(f"Youtube API Rate limit exceeded.")
                break
            progress.text(f"Upload progress: {i}/{len(df)}")
            i+=1
            os.remove(video_file)

import boto3
from botocore.exceptions import ClientError
import streamlit as st
import time
from datetime import datetime
import pytz

# Global variables for AWS clients
s3 = None
mediaconvert = None

def s3_wait_for_job_completion(mediaconvert, job_id):
    est = pytz.timezone('US/Eastern')
    
    while True:
        try:
            response = mediaconvert.get_job(Id=job_id)
            status = response['Job']['Status']
            current_time = datetime.now(est).strftime('%Y-%m-%d %H:%M:%S EST')
            
            if status == 'COMPLETE':
                print(f"[{current_time}] Job {job_id} completed successfully!")
                return True
            elif status == 'ERROR':
                print(f"[{current_time}] Job {job_id} failed. Error message: {response['Job']['ErrorMessage']}")
                return False
            else:
                print(f"[{current_time}] Job {job_id} is {status}. Waiting...")
                time.sleep(30)  # Wait for 30 seconds before checking again
        except ClientError as e:
            current_time = datetime.now(est).strftime('%Y-%m-%d %H:%M:%S EST')
            print(f"[{current_time}] An error occurred while checking job status: {e}")
            return False

def read_aws_credentials(file_path):
    credentials = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split(' = ')
                credentials[key] = value.strip("'")
    except FileNotFoundError:
        st.error(f"AWS credentials file not found: {file_path}")
    except Exception as e:
        print(f"Error reading AWS credentials: {str(e)}")
    return credentials

def initialize_aws_clients():
    global s3, mediaconvert
    
    # Read AWS credentials
    aws_credentials = read_aws_credentials("amazon.txt")

    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    try:
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION_NAME
        )

        # Initialize S3 and MediaConvert clients
        s3 = session.client('s3')
        mediaconvert = session.client('mediaconvert', endpoint_url='https://wa11sy9gb.mediaconvert.us-east-2.amazonaws.com')
    except Exception as e:
        st.error(f"Error initializing AWS services: {str(e)}")
        st.stop()

# Initialize AWS clients
initialize_aws_clients()

BUCKET_NAME = 'video-stitch'

st.header("S3 Video Stitching Tool")

# Add text input fields for folder names
input_folder = st.text_input("Enter the folder name containing intro and main videos:")
output_folder = st.text_input("Enter the output folder name in the video-stitch bucket:")

def list_s3_files(bucket_name, prefix=''):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [item['Key'] for item in response.get('Contents', [])]
    except ClientError as e:
        st.error(f"Error listing S3 files: {e}")
        return []

def match_video_pairs(files):
    pairs = []
    
    # First, match _intro, _main, and _judge pairs
    intro_videos = [f for f in files if f.endswith('_intro.mp4')]
    main_videos = [f for f in files if f.endswith('_main.mp4')]
    judge_videos = [f for f in files if f.endswith('_judge.mp4')]
    
    for intro in intro_videos:
        name = intro.split('_')[0]
        main = next((m for m in main_videos if m.startswith(name)), None)
        judge = next((j for j in judge_videos if j.startswith(name)), None)
        if main:
            pairs.append((intro, main, judge))
    
    return pairs

def create_mediaconvert_job(input_key1, input_key2, input_key3, output_key):
    inputs = [
        {
            "AudioSelectors": {
                "Audio Selector 1": {
                    "DefaultSelection": "DEFAULT"
                }
            },
            "VideoSelector": {},
            "TimecodeSource": "ZEROBASED",
            "FileInput": f"s3://{BUCKET_NAME}/{input_key1}"
        },
        {
            "AudioSelectors": {
                "Audio Selector 1": {
                    "DefaultSelection": "DEFAULT"
                }
            },
            "VideoSelector": {},
            "TimecodeSource": "ZEROBASED",
            "FileInput": f"s3://{BUCKET_NAME}/{input_key2}"
        }
    ]

    if input_key3:
        inputs.append(
            {
                "AudioSelectors": {
                    "Audio Selector 1": {
                        "DefaultSelection": "DEFAULT"
                    }
                },
                "VideoSelector": {},
                "TimecodeSource": "ZEROBASED",
                "FileInput": f"s3://{BUCKET_NAME}/{input_key3}"
            }
        )

    inputs.append(
        {
            "AudioSelectors": {
                "Audio Selector 1": {
                    "DefaultSelection": "DEFAULT"
                }
            },
            "VideoSelector": {},
            "TimecodeSource": "ZEROBASED",
            "FileInput": "s3://li-general-task/input_videos/outro.mp4"
        }
    )

    job_settings = {
        "Inputs": inputs,
        "OutputGroups": [
            {
                "Name": "File Group",
                "OutputGroupSettings": {
                    "Type": "FILE_GROUP_SETTINGS",
                    "FileGroupSettings": {
                        "Destination": f"s3://{BUCKET_NAME}/{output_key}"
                    }
                },
                "Outputs": [
                    {
                        "VideoDescription": {
                            "CodecSettings": {
                                "Codec": "H_264",
                                "H264Settings": {
                                    "RateControlMode": "CBR",
                                    "Bitrate": 5000000
                                }
                            }
                        },
                        "AudioDescriptions": [
                            {
                                "CodecSettings": {
                                    "Codec": "AAC",
                                    "AacSettings": {
                                        "Bitrate": 96000,
                                        "CodingMode": "CODING_MODE_2_0",
                                        "SampleRate": 48000
                                    }
                                }
                            }
                        ],
                        "ContainerSettings": {
                            "Container": "MP4",
                            "Mp4Settings": {}
                        }
                    }
                ]
            }
        ]
    }

    try:
        response = mediaconvert.create_job(
            Role='arn:aws:iam::339713096623:role/MediaConvertRole',
            Settings=job_settings
        )
        return response['Job']['Id']
    except ClientError as e:
        st.error(f"Error creating MediaConvert job: {e}")
        return None

# The main part of your script should be updated as follows:
if st.button("Start Concatenation"):
    if not input_folder:
        st.warning("Please enter the folder name containing intro and main videos.")
    elif not output_folder:
        st.warning("Please enter an output folder name.")
    else:
        with st.spinner("Stitching videos..."):
            files = list_s3_files(BUCKET_NAME, prefix=input_folder)
            
            print("Debug: All files in the bucket")
            if files:
                for file in files:
                    print(file)
            else:
                print("No files found in the bucket.")
            
            pairs = match_video_pairs(files)
            
            print("Debug: Matched video pairs")
            if pairs:
                for intro, main in pairs:
                    print(f"Intro: {intro} - Main: {main}")
            else:
                print("No matching video pairs found.")
            
            if not pairs:
                st.warning("No matching video pairs found in the S3 bucket.")
            else:
                progress_bar = st.progress(0)
                for i, (intro, main) in enumerate(pairs):
                    try:
                        name = intro.split('_')[0]
                        if name.lower().endswith('.mp4'):
                            name = name[:-4]
                        output_key = f"{output_folder}/{name}_final.mp4"
                        
                        print(f"Processing: Intro - {intro}, Main - {main}")
                        print(f"Output: {output_key}")

                        job_id = create_mediaconvert_job(intro, main, output_key)
                        if job_id:
                            print(f"MediaConvert job created with ID: {job_id}. (Video #{i+1} for {name})")
                            time.sleep(5)
                            # add this part back in if you wish to see detailed updates of the job progress
                            # job_successful = s3_wait_for_job_completion(mediaconvert, job_id)
                            # if job_successful:
                            #     print("Video processing completed successfully!")
                            # else:
                            #     print("Video processing failed. Please check the AWS Console for more details.")
                        else:
                            print("Failed to create MediaConvert job.")
                            progress_bar.progress((i + 1) / len(pairs))
                    except:
                        print(f"Error processing line {i+1}.")
                        progress_bar.progress((i + 1) / len(pairs))
                
                st.success("Video stitching jobs submitted successfully!")

st.header("Presentation Downloader Tool")

video_csv = st.file_uploader(label="Upload a CSV file of videos to be stitched", type=['csv'])

download_videos = st.button("Download Videos")

def download_file_from_google_drive(file_id, output_path, drive_service):
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    with open(output_path, 'wb') as f:
        f.write(fh.read())

def extract_file_id(url):
    return url.split('/')[-2]

if (video_csv and st.session_state['final_auth']) and download_videos:
    with st.spinner("Downloading videos..."):
        st.session_state['creds']['refresh_token'] = st.session_state['creds']['_refresh_token']
        creds = Credentials.from_authorized_user_info(st.session_state['creds'])
        drive_service = build('drive', 'v3', credentials=creds)

        df = pd.read_csv(video_csv)
        
        if not all(column in df.columns for column in ['name', 'intro', 'main']):
            st.error("CSV must contain 'name', 'intro', and 'main' columns.")
        else:
            # Create directories if they don't exist
            os.makedirs('intro_videos', exist_ok=True)
            os.makedirs('main_videos', exist_ok=True)
            os.makedirs('judge_videos', exist_ok=True)

            total_rows = len(df)
            progress_bar = st.progress(0)
            status_text = st.empty()

            for index, row in df.iterrows():
                name = row['name']
                intro_url = row['intro']
                main_url = row['main']
                judge_url = row.get('judge', None)

                # Download intro video
                intro_file_id = extract_file_id(intro_url)
                intro_output_path = os.path.join('intro_videos', f"{name}_intro.mp4")
                download_file_from_google_drive(intro_file_id, intro_output_path, drive_service)
                print(f"Row {index + 1}/{total_rows}: Downloaded intro video for {name}")

                # Download main video
                main_file_id = extract_file_id(main_url)
                main_output_path = os.path.join('main_videos', f"{name}_main.mp4")
                download_file_from_google_drive(main_file_id, main_output_path, drive_service)
                print(f"Row {index + 1}/{total_rows}: Downloaded main video for {name}")

                # Download judge video if it exists
                if judge_url:
                    judge_file_id = extract_file_id(judge_url)
                    judge_output_path = os.path.join('judge_videos', f"{name}_judge.mp4")
                    download_file_from_google_drive(judge_file_id, judge_output_path, drive_service)
                    print(f"Row {index + 1}/{total_rows}: Downloaded judge video for {name}")

                # Update progress
                progress = (index + 1) / total_rows
                progress_bar.progress(progress)
                status_text.text(f"Downloaded videos for {index + 1} out of {total_rows} presentations")

            st.success("All videos have been downloaded successfully!")