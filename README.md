# SOURCE

## 0. Setup
- Install Google Cloud CLI
- Login to GCP with command `gcloud auth application-default login`
- Export Environment Variable `GOOGLE_APPLICATION_CREDENTIALS` which path of `~/.config/gcloud/application_default_credentials.json`

## 1. Export Data to JSONL file:
- Script: [export_and_upload_to_GCS/export_to_JSONL.py](src/export_and_upload_to_GCS/export_to_JSONL.py)
- Parameters: `None`

### **Workflow**

## 2. Upload file to Google Cloud Storage: 
- Script: [export_and_upload_to_GCS/upload_to_GCS.py](src/export_and_upload_to_GCS/upload_to_GCS.py)
- Parameters:
  1. File path: File or Folder to upload to GCS
  2. Bucket name: Destination Bucket in GCS
- Example command: `python [project_path]/src/export_and_upload_to_GCS/upload_to_GSC.py [file_path] [bucket_name]`

### **Workflow**

## * Export from MongoDB + Upload to GCS:
- script: [export_and_upload_to_GCS/main.py](src/export_and_upload_to_GCS/main.py)
- Parameters: Bucket name: Destination Bucket in GCS
- Example command: `python [project_path]/src/export_and_upload_to_GCS/main.py [bucket_name]`

### **Workflow**