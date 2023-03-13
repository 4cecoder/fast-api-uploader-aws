from fastapi import FastAPI, File, UploadFile
import boto3
import uvicorn
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(
    title="Fast Uploader",
    description="A fast endpoint for uploading files to AWS S3.",
    version="1.0.0",
)

BUCKET_NAME = 'your-bucket-name'

s3 = boto3.client('s3',
                  aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))

@app.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    try:
        if file.content_length > 3 * 1024 * 1024 * 1024:
            raise ValueError("File size is too large")

        with ThreadPoolExecutor() as executor:
            executor.submit(upload_file_to_s3, file.file, file.filename)

        print(f"File {file.filename} uploaded successfully to S3")
        return {"filename": file.filename}

    except Exception as e:
        print(f"Error uploading file {file.filename}: {str(e)}")
        return {"error": str(e)}

def upload_file_to_s3(file, filename):
    try:
        s3.upload_fileobj(file, BUCKET_NAME, filename)
        print(f"File {filename} uploaded successfully to S3")
    except Exception as e:
        print(f"Error uploading file {filename}: {str(e)}")
        raise e

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
