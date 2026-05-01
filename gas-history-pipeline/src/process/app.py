import os
import logging
from io import BytesIO
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import pandas as pd
import xlrd


# ------------------------------------------------------------
# LOGGING SETUP
# ------------------------------------------------------------
# Just like in the fetch Lambda, we create one logger for the module.
# This keeps logging consistent across the file and sends output to
# CloudWatch Logs automatically when the function runs in AWS.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ------------------------------------------------------------
# AWS CLIENT / RESOURCE SETUP
# ------------------------------------------------------------
# These are created outside the handler so they can potentially be reused
# across warm Lambda invocations.
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# ------------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------------
# These values come from template.yaml.
# The processor needs to know:
# - which DynamoDB table to write normalized weekly gas-price records into
# - which worksheet to read from the workbook
# - which source-key column to extract from that worksheet
# - what human-readable source name to store in normalized records
GAS_HISTORY_TABLE_NAME = os.environ["GAS_HISTORY_TABLE_NAME"]
TARGET_SHEET_NAME = os.environ["TARGET_SHEET_NAME"]
TARGET_SOURCE_KEY = os.environ["TARGET_SOURCE_KEY"]
SOURCE_NAME = os.environ["SOURCE_NAME"]

# Create a DynamoDB Table object for later writes.
gas_history_table = dynamodb.Table(GAS_HISTORY_TABLE_NAME)


# ------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------
def parse_s3_event(event: dict) -> tuple[str, str]:
    """
    Extract the bucket name and object key from an S3 object-created event.

    Why this helper exists:
    - The process Lambda is triggered by S3, so its event shape is different
      from the fetch Lambda's schedule event.
    - Keeping this logic in a helper makes the main handler cleaner.
    - It also makes it easier to test just the event-parsing logic.

    Expected shape:
    event["Records"][0]["s3"]["bucket"]["name"]
    event["Records"][0]["s3"]["object"]["key"]

    For this project, one uploaded raw workbook should trigger one processing
    run, so using the first record is a reasonable starting assumption.
    """
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]
    return bucket_name, object_key


# ------------------------------------------------------------
# LAMBDA HANDLER
# ------------------------------------------------------------
def lambda_handler(event, context):
    """
    Main entry point for the process Lambda.

    Final high-level flow:
    1. Parse the S3 event.
    2. Download the raw workbook from S3.
    3. Load the target worksheet.
    4. Extract the configured target source-key series.
    5. Normalize the extracted rows into final record shape.
    6. Write the normalized records into DynamoDB.

    This completes the processing side of the pipeline.
    """
    logger.info("ProcessGasHistoryFunction invoked.")

    bucket_name, object_key = parse_s3_event(event)

    logger.info("Received S3 event for bucket: %s", bucket_name)
    logger.info("Received S3 event for object key: %s", object_key)

    workbook_bytes = download_raw_workbook(bucket_name, object_key)
    df = load_target_sheet_from_workbook(workbook_bytes)

    series_df = extract_target_seriers(df)
    normalized_records = normalize_weekly_records(series_df, object_key)

    write_records_to_dynamodb(normalized_records)

    return {
        "status": "SUCCESS",
        "bucket_name": bucket_name,
        "object_key": object_key,
        "target_sheet_name": TARGET_SHEET_NAME,
        "target_source_key": TARGET_SOURCE_KEY,
        "gas_history_table_name": GAS_HISTORY_TABLE_NAME,
        "record_count_written": len(normalized_records),
    }
    
def download_raw_workbook(bucket_name: str, object_key: str) -> bytes:
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        workbook_bytes = response["Body"].read()
        
        logger.info(
            "Downloaded raw workbook from s3://%s/%s (%s bytes)",
            bucket_name,
            object_key,
            len(workbook_bytes),
        )
        
        return workbook_bytes
    
    except Exception:
        logger.exception(
            "Failed to download raw workbook from s3://%s/%s (%s bytes)",
            bucket_name,
            object_key, 
        )
        raise
        
def load_target_sheet_from_workbook(workbook_bytes: bytes) -> pd.DataFrame:
    # Loads the worksheet from the Excel workbook into a DataFrame
    
    workbook_stream = BytesIO(workbook_bytes)
    
    try:
        
        df = pd.read_excel(
            workbook_stream,
            sheet_name=TARGET_SHEET_NAME,
            header=None,
            engine="xlrd",
        ) 
           
        logger.info(
            "Loaded worksheet '%s' successfully with shape rows=%s, cols=%s",
            TARGET_SHEET_NAME,
            df.shape[0],
            df.shape[1],
        )
        
        return df
    
    except Exception:
        logger.exception(
            "Failed to load worksheet '%s' from raw workbook.",
            TARGET_SHEET_NAME,
        )
        raise

def extract_target_seriers(df: pd.DataFrame) -> pd.DataFrame:
    # Extract the data column and target source key column from the worksheet
    
    sourcekey_row = df.iloc[1]
    
    # Find every column index whose value matches the configured target source key.
    matching_columns = sourcekey_row[sourcekey_row == TARGET_SOURCE_KEY].index.tolist()
    if not matching_columns:
        raise ValueError(
            f"Could not find target source key '{TARGET_SOURCE_KEY}' in worksheet '{TARGET_SHEET_NAME}'."
        )
    # Get only one correcrt target column from the dataset
    target_column_index = matching_columns[0]
    
    logger.info(
        "Found target source key '%s' in column index %s/",
        TARGET_SOURCE_KEY,
        target_column_index,
    )        
    
    #Skip top metadata rows and sstart from row 3, where the real data begins.
    series_df = df.iloc[3:, [0, target_column_index]].copy()
    
    # Give the columns clear temporary names
    series_df.columns = ["week_date_raw", "price_raw"]
    
    series_df = series_df.dropna(subset=["week_date_raw", "price_raw"])
    
    logger.info(
        "Extracted target series with %s non-empty weekly rows.",
        len(series_df),
    )
    
    return series_df

def preview_target_series(series_df: pd.DataFrame, preview_size: int = 5) -> list[dict]:
    # Return a small preview of the extracted target series for testing/debugging.
    
    preview_df = series_df.head(preview_size).copy()
    return preview_df.to_dict(orient="records")
    

def normalize_weekly_records(series_df: pd.DataFrame, raw_file_s3_key: str) -> list[dict]:
    # Convert the extractred target series DataFrame into normalized
    # DynamoDb-ready record dictionaries.
    
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    # Convert the raw date column into a stable YYYY-MM-DD string.
    
    series_df = series_df.copy()
    series_df["week_date"] = pd.to_datetime(series_df["week_date_raw"]).dt.strftime("%Y-%m-%d")
    
    records = []
    
    for row in series_df.itertuples(index=False):
        record = {
            "week_date": row.week_date,
            "national_avg_gas_price": Decimal(str(row.price_raw)),
            "source": SOURCE_NAME,
            "source_key": TARGET_SOURCE_KEY,
            "raw_file_s3_key": raw_file_s3_key,
            "ingested_at": ingested_at,
        }
        
        records.append(record)
        
    logger.info("Normalized %s weekly gas-price records.", len(records))
    return records

def preview_normalized_records(records: list[dict], preview_size: int = 5) -> list[dict]:
    # Return a small preview of the normalized records. 
    return records[:preview_size]

def write_records_to_dynamodb(records: list[dict]) -> None:
    # Write normalized weekly gas-history records into DynamoDB.
    
    try:
        with gas_history_table.batch_writer() as batch:
            for record in records:
                batch.put_item(Item=record)
                
        logger.info(
            "successfully wrote %s normalized weekly records to DynamoDB table '%s'.",
            len(records),
            GAS_HISTORY_TABLE_NAME,
        )
        
    except Exception:
        logger.exception(
            "Failed while writing normalized weekly records to DynamoDB."
        )
        raise