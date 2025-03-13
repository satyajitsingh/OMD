import json
import os
import time
import boto3
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler



# MinIO Configuration
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET_NAME = "data-feed"
SERVICE_NAME = "data-storage"
WATCH_FOLDER = "/watched_folder"
# Container details
OM_URL = "http://localhost:8585/api/v1"
CONTAINER_NAME = BUCKET_NAME
CONTAINER_SERVICE = "my_container_service"  # Reference the container service

# OpenMetadata Configuration
OMD_API_URL = "http://openmetadata-server:8585/api/v1"
# "http://openmetadata_server:8585/api/v1"
OMD_AUTH_TOKEN = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImFkbWluIiwicm9sZXMiOlsiQWRtaW4iXSwiZW1haWwiOiJhZG1pbkBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiUEVSU09OQUxfQUNDRVNTIiwiaWF0IjoxNzQxNzA3ODkyLCJleHAiOjE3NDk0ODM4OTJ9.Dfdi2jZ8HeCByYtqis4iJjKdydQwpbCd-2zSTBDV5E0eDQTTKKzPrviCFb6yEoPXbB33QYrwX_VZvQkNCKiPzK_GOdP9CtOoAIrpZw60cOd9vg4SuUXA4qoxjBoP7DWe86kF0irEhmuiC5iSw4rDwIAe5b-dbs2ihE4Q3TkpJUSCxoxM0Eg8xjeGydOUfnV5Wti2QWW15JD8a1SB7jCe-hsmHA21RWXKlVHAcGkjZydFU8SMuKdFc08_llcdZ_ii0t71iJmH9nxv-auZeQ6YVuMkkjH88F2Rs_GySwPH14Ah_2mADatTRy7dam0TX8yksdHpiPOMF2ZIwOj2LxQCSQ"
#eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6InNhdHlhaml0X3NpbmdoIiwicm9sZXMiOltdLCJlbWFpbCI6InNhdHlhaml0X3NpbmdoQHlhaG9vLmNvbSIsImlzQm90IjpmYWxzZSwidG9rZW5UeXBlIjoiUEVSU09OQUxfQUNDRVNTIiwiaWF0IjoxNzQxMDg1NjA0LCJleHAiOjE3NDYyNjk2MDR9.nLCw1V1_8T1rriHj6_KiNoPljuiiRREtWxPdHY_JHXBtLjSwa0PVbURp6DVnXe78osqV3_KDqzLC61c96PzOOv8C9VlrDQg-Yaj5PCyfjTXemkRYJOm3aCWYtQmsvbpB9tjNe2LvM_XHLlucjV0wwOwaOAyAkbBWnBYmRveQImpsHwB0-MRhIqD4TjkP3fVtPJw7Dm4kJMDgZXg7_U-4WB75w__6EVXjC1tr-gk-NtZiLxu63Et1GIS8nHfvg54iLeE2HoQrrN_C6iqbCFGqPJq48iCxa13Uq3DnvNqnvm0gcRwtc4LsVsO0xanW16HxjlE0FVfDdirtD2OEGazy0w"  # Generate in OpenMetadata UI

# Message to be added under relevant folder
CUSTOM_MESSAGE = "This folder contains uploaded files from the monitoring script."
# Headers for API requests
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OMD_AUTH_TOKEN}"  # Remove if auth is not needed
}

# Initialize MinIO Client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure the MinIO bucket exists
def create_minio_bucket():
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created successfully.", flush=True)
        return True
    except Exception as e:
        print(f"Bucket already exists: {e}", flush=True)
        return True
    

# Check if file exists in MinIO bucket
def file_exists_in_minio(file_name):
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=file_name)
        # If the file exists
        return True
    except Exception as e:
        print(f"File {file_name} does not exist in MinIO: {e}", flush=True)
        return False

# Upload file to MinIO
def upload_to_minio(file_path):
    relative_path = os.path.relpath(file_path, WATCH_FOLDER)  # Preserve folder structure
    minio_key = relative_path.replace("\\", "/")  # Ensure UNIX-style paths
    
    if file_exists_in_minio(minio_key):
        print(f"⏩ Skipping {minio_key}, already in MinIO.")
        return None
    
    try:
        print(f"Uploading {minio_key} to MinIO bucket '{BUCKET_NAME}'")
        s3_client.upload_file(file_path, BUCKET_NAME, minio_key)
        print(f"Uploaded {minio_key} to MinIO bucket '{BUCKET_NAME}'")
        return f"{MINIO_URL}/{BUCKET_NAME}/{minio_key}"
    
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        return None


# Check if a service exists in OpenMetadata
def check_service_exists():
    url = f"{OMD_API_URL}/services/storage"
    headers = {"Authorization": f"Bearer {OMD_AUTH_TOKEN}"}
    try:
        response = requests.get(url, headers=headers)
        services = response.json().get("data", [])
        for service in services:
            if service.get("name") == BUCKET_NAME:
                print(f"service: {BUCKET_NAME} found")
                return True
    except Exception as e:
        print(f"⚠️ Error checking service: {e}")

    return False



# Create a new service in OpenMetadata
def check_or_create_container(service_name):
    print(f"Service '{service_name}' check.")
    """ Check if OpenMetadata has a container matching the MinIO bucket, if not create one """
    headers = {"Authorization": f"Bearer {OMD_AUTH_TOKEN}", "Content-Type": "application/json"}
    
    # check if service exists
    response = requests.get(f"{OMD_API_URL}/services/storageServices/name/{service_name}", headers=HEADERS)
    print(response.status_code)
   
    if response.status_code == 200:
        print(f"Service '{service_name}' exists.")
        return True
    elif response.status_code == 404:
        print(f"Creating Storage Service")
        dummy_service_payload = {
        "name": {SERVICE_NAME},
        "serviceType": "S3",
        "connection": {
            "config": {
                "type": "S3",
                "awsConfig": {  # ✅ Correct structure for credentials
                    "awsAccessKeyId": "dummy-key",
                    "awsSecretAccessKey": "dummy-secret"
                }
            }
        },
        "description": "A dummy storage service for container creation"
    }
        dummy_service_url = f"{OMD_API_URL}/services/storageServices"
        dummy_response = requests.post(dummy_service_url, headers=headers, data=json.dumps(dummy_service_payload))
        print("Dummy service creation:", dummy_response.status_code, dummy_response.text)
        return True

def create_container():
    
    
#     # Check if container exists
#     try:
        headers = {"Authorization": f"Bearer {OMD_AUTH_TOKEN}", "Content-Type": "application/json"}
        response = requests.get(f"{OMD_API_URL}/containers/name/{BUCKET_NAME}", headers=headers)
        if response.status_code == 200:
            print(f"Container '{BUCKET_NAME}' exists.")
            return True
        elif response.status_code == 404:
            print(f"Container '{BUCKET_NAME}' not found. Creating...")
            container_payload = {
                "name": "data-feed",
                "description": "Container for data feed using dummy storage service",
                "service": "data-storage",  # Ensure this matches an existing storage service
                "dataModel": {
                    "columns": [
                        {"name": "file_name", "dataType": "STRING"},
                        {"name": "upload_time", "dataType": "TIMESTAMP"}
                    ]
                }
            }
            
            for key, value in container_payload.items():
                if isinstance(value, set):
                    container_payload[key] = list(value)  # Convert sets to lists for serialization
            try:
                print(json.dumps(container_payload, indent=2))  # ✅ Check JSON format
            except TypeError as e:
                print("JSON serialization error:", e)
            container_url = f"{OMD_API_URL}/containers"
            container_response = requests.post(container_url, headers=headers, data=json.dumps(container_payload))
            print("Container creation:", container_response.status_code, container_response.text)
            return True

def add_message_to_container(bucket_name, object_path):
    """ Add a custom message under the relevant folder in OpenMetadata """
    headers = {"Authorization": f"Bearer {OMD_AUTH_TOKEN}", "Content-Type": "application/json"}
    
    # Construct metadata entry
    metadata_entry = {
        "name": object_path,
        "description": CUSTOM_MESSAGE,
        "databaseSchema": bucket_name
    }
    
    # Add metadata to OpenMetadata
    response = requests.post(f"{OMD_API_URL}/tables", headers=headers, data=json.dumps(metadata_entry))
    if response.status_code == 201:
        print(f"Metadata added for '{object_path}' in container '{bucket_name}'.")
    else:
        print(f"Failed to add metadata: {response.text}")


    # container_data = {
    #     "name": BUCKET_NAME,
    #     "description": "A test container in OpenMetadata",
    #     "service": CONTAINER_SERVICE,  # Reference to the container service
    #     "dataModel": {
    #         "jsonSchema": {}  # Define schema if needed
    #     }
    # }
    # url = f"{OMD_API_URL}/services/storage"
    # headers = {"Authorization": f"Bearer {OMD_AUTH_TOKEN}", "Content-Type": "application/json"}
    # payload = {
    #     "name": BUCKET_NAME,
    #     "serviceType": "S3",
    #     "description": f"Storage service for bucket {BUCKET_NAME}",
    #     "connection": {
    #         "config": {
    #             "type": "s3",
    #             "endpointURL": MINIO_URL,
    #             "awsAccessKeyId": MINIO_ACCESS_KEY,
    #             "awsSecretAccessKey": MINIO_SECRET_KEY,
    #             "bucketName": BUCKET_NAME,
                
    #         }
    #     }
    # }
    
    # try:
    #     response = requests.post(url, json=payload, headers=headers)
    #     if response.status_code == 200:
    #         print(f"✅ Created service '{BUCKET_NAME}' in OpenMetadata.")
    #     else:
    #         print(f"❌ Failed to create service: {response.text}")
    # except Exception as e:
    #     print(f"⚠️ Error creating service: {e}")

# Send data feed to OpenMetadata
# def send_data_feed(file_name, file_url):
#     url = f"{OMD_API_URL}/feed"
#     headers = {"Authorization": f"Bearer {OMD_AUTH_TOKEN}", "Content-Type": "application/json"}
#     payload = {
#         "from": "File Watcher",
#         "message": f"New file uploaded: {file_name}",
#         "threadId": BUCKET_NAME,
#         "about": f"File available at {file_url}"
#     }
    
    
#     try:
#         response = requests.post(url, json=payload, headers=headers)
#         if response.status_code == 200:
#             print(f"Data feed sent to OpenMetadata: {file_name}", flush=True)
#         else:
#             print(f"Failed to send data feed: {response.text}", flush=True)
#     except Exception as e:
#         print(f"Error sending data feed: {e}", flush=True)

# # Watchdog Event Handler
# class FileWatcher(FileSystemEventHandler):
#     def on_created(self, event):
#         if not event.is_directory:
#             print(f"[DEBUG] File created: {event.src_path}",flush=True)
#             time.sleep(50)  # Small delay to avoid race condition
#             file_url = upload_to_minio(event.src_path)
#             if file_url:
#                 send_data_feed(os.path.basename(event.src_path), file_url)


def poll_folder():
    try:
        create_minio_bucket()
        check_or_create_container(SERVICE_NAME)
        create_container()
    except Exception as e:
        print(f"⚠️ Error in setup: {e}")
    

    print(f" Watching folder: {WATCH_FOLDER}")
    
    # while True:
    #     try:
    #         for root, _, files in os.walk(WATCH_FOLDER):
    #             for file in files:
    #                 file_path = os.path.join(root, file)
    #                 file_url = upload_to_minio(file_path)
    #                 if file_url:
    #                     add_message_to_container(BUCKET_NAME,file_url)

    #         time.sleep(10)  # Poll every 10 seconds
    #     except KeyboardInterrupt:
    #         print("❌ Stopping folder watch.")
    #         break
    #     except Exception as e:
    #         print(f"⚠️ Error in polling loop: {e}")
        


# # Start Watching Folder
# def watch_folder(folder_path):
#     event_handler = FileWatcher()
#     observer = Observer()
#     observer.schedule(event_handler, folder_path, recursive=True)
#     observer.start()
#     print(f"Watching folder: {folder_path}")

#     try:
#         while True:
#             time.sleep(10)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()

if __name__ == "__main__":
    # watch_folder("/watched_folder")  # Change path if running locally

    poll_folder() 
