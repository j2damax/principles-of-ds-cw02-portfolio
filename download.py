import csv
import boto3
from pyairtable import Table
import pandas as pd
from io import StringIO

# Airtable API details
API_KEY = "" # REMOVED FOR SCREEN CAPTURE
BASE_ID = "apprqf6ijdtKiSNF3"
TABLE_NAME = "Score"

# AWS S3 details
AWS_ACCESS_KEY = "" # REMOVED FOR SCREEN CAPTURE
AWS_SECRET_KEY = "" # REMOVED FOR SCREEN CAPTURE
BUCKET_NAME = "jam-dataset-msc"
S3_FILE_PATH = "challenges_completed_2024.csv"  

# Initialize Airtable Table
table = Table(API_KEY, BASE_ID, TABLE_NAME)

# List to store all records
all_records = []

# Paginated fetch (100 records per request)
for record in table.iterate(view="main", page_size=100):
    all_records.append(record["fields"]) 

# Convert data to Pandas DataFrame
df = pd.DataFrame(all_records)

# Convert DataFrame to CSV (in-memory)
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue()

# Upload CSV to S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

try:
    s3_client.put_object(Bucket=BUCKET_NAME, Key=S3_FILE_PATH, Body=csv_data)
    print(f"✅ File uploaded successfully to s3://{BUCKET_NAME}/{S3_FILE_PATH}")
except Exception as e:
    print(f"⚠️ Failed to upload file: {e}")
