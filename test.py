import boto3
import botocore
import time

def read_aws_credentials(file_path):
    aws_credentials = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip().replace('\r', '')
                if ' = ' in line:
                    key, value = line.split(' = ')
                    aws_credentials[key] = value.strip("'")
    except FileNotFoundError:
        print(f"{file_path} not found. Please ensure it exists in the correct location.")
        exit(1)
    except Exception as e:
        print(f"An error occurred while reading the file: {str(e)}")
        exit(1)
    return aws_credentials

def create_mediaconvert_job(mediaconvert, job_settings):
    try:
        response = mediaconvert.create_job(
            Role='arn:aws:iam::339713096623:role/MediaConvertRole',
            Settings=job_settings
        )
        return response['Job']['Id']
    except botocore.exceptions.ClientError as e:
        print(f"An error occurred while creating the job: {e}")
        return None

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
            print(f"An error occurred while checking job status: {e}")
            return False

def main():
    aws_credentials = read_aws_credentials("amazon.txt")

    AWS_REGION_NAME = aws_credentials.get('AWS_REGION_NAME', 'Default-Region')
    AWS_ACCESS_KEY = aws_credentials.get('AWS_ACCESS_KEY', 'Default-Access-Key')
    AWS_SECRET_KEY = aws_credentials.get('AWS_SECRET_KEY', 'Default-Secret-Key')

    BUCKET_NAME = 'video-stitch'  # This is now the bucket name

    # Ask for input file names
    input_file_name1 = input("Enter the name of your first input video file: ")
    input_file_name2 = input("Enter the name of your second input video file: ")
    output_file_name = input("Enter the desired name for your output video file: ")

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION_NAME
    )

    try:
        mediaconvert = session.client('mediaconvert')
        endpoints = mediaconvert.describe_endpoints()
        mediaconvert = session.client('mediaconvert', 
                                      endpoint_url=endpoints['Endpoints'][0]['Url'])

        print("MediaConvert client created successfully!")

        # Define your job settings here
        job_settings = {
            "Inputs": [
                {
                    "AudioSelectors": {
                        "Audio Selector 1": {
                            "DefaultSelection": "DEFAULT"
                        }
                    },
                    "VideoSelector": {},
                    "TimecodeSource": "ZEROBASED",
                    "FileInput": f"s3://{BUCKET_NAME}/{input_file_name1}"
                },
                {
                    "AudioSelectors": {
                        "Audio Selector 1": {
                            "DefaultSelection": "DEFAULT"
                        }
                    },
                    "VideoSelector": {},
                    "TimecodeSource": "ZEROBASED",
                    "FileInput": f"s3://{BUCKET_NAME}/{input_file_name2}"
                }
            ],
            "OutputGroups": [
                {
                    "Name": "File Group",
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": f"s3://{BUCKET_NAME}/{output_file_name}"
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

        # Print the input and output file paths for verification
        print(f"Input file 1 path: s3://{BUCKET_NAME}/{input_file_name1}")
        print(f"Input file 2 path: s3://{BUCKET_NAME}/{input_file_name2}")
        print(f"Output file path: s3://{BUCKET_NAME}/{output_file_name}")

        job_id = create_mediaconvert_job(mediaconvert, job_settings)
        if job_id:
            print(f"MediaConvert job created with ID: {job_id}")
            job_successful = wait_for_job_completion(mediaconvert, job_id)
            if job_successful:
                print("Video processing completed successfully!")
            else:
                print("Video processing failed. Please check the AWS Console for more details.")
        else:
            print("Failed to create MediaConvert job.")

    except botocore.exceptions.ClientError as e:
        print(f"An AWS error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()
