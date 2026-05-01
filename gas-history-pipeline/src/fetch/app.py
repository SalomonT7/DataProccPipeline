import os
import logging
import hashlib
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import boto3
from botocore.exceptions import ClientError

# ------------------------------------------------------------
# LOGGING SETUP
# ------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ------------------------------------------------------------
# AWS CLIENT / RESOURCE SETUP
# ------------------------------------------------------------
dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

# ------------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------------
TRACKING_TABLE_NAME = os.environ["TRACKING_TABLE_NAME"]
RAW_BUCKET_NAME = os.environ["RAW_BUCKET_NAME"]
DATASET_NAME = os.environ["DATASET_NAME"]
SOURCE_URL = os.environ["SOURCE_URL"]
RAW_PREFIX = os.environ["RAW_PREFIX"]

# DynamoDB table object
tracking_table = dynamodb.Table(TRACKING_TABLE_NAME)


# ------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------
def utc_now_iso() -> str:
    """Return the current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def get_tracking_record(dataset_name: str) -> dict:
    """Read the current tracking record from DynamoDB."""
    try:
        response = tracking_table.get_item(Key={"dataset_name": dataset_name})
        return response.get("Item", {})
    except ClientError:
        logger.exception("Failed to read tracking record from DynamoDB.")
        raise


def save_tracking_record(existing_record: dict, updates: dict) -> None:
    """Merge old and new tracking values and save them."""
    merged_record = {
        **existing_record,
        **updates,
        "dataset_name": DATASET_NAME,
    }

    try:
        tracking_table.put_item(Item=merged_record)
        logger.info(
            "Tracking record saved for dataset '%s' with status '%s'.",
            DATASET_NAME,
            merged_record.get("status"),
        )
    except ClientError:
        logger.exception("Failed to save tracking record to DynamoDB.")
        raise


def try_fetch_source_metadata(url: str) -> dict:
    """Try a lightweight HEAD request to get source metadata."""
    request = Request(url, method="HEAD")

    try:
        with urlopen(request, timeout=30) as response:
            return {
                "last_source_modified": response.headers.get("Last-Modified"),
                "etag": response.headers.get("ETag"),
            }
    except Exception as exc:
        logger.warning("HEAD metadata check failed or returned nothing useful: %s", exc)
        return {}


def download_source_file(url: str) -> bytes:
    """Download the full source workbook and return raw bytes."""
    request = Request(url)

    try:
        with urlopen(request, timeout=60) as response:
            file_bytes = response.read()

        logger.info("Downloaded source file successfully. Size: %s bytes", len(file_bytes))
        return file_bytes

    except HTTPError:
        logger.exception("HTTP error while downloading source file.")
        raise

    except URLError:
        logger.exception("Network/URL error while downloading source file.")
        raise


def sha256_hex(file_bytes: bytes) -> str:
    """Compute a SHA-256 hash of the workbook bytes."""
    return hashlib.sha256(file_bytes).hexdigest()


def build_raw_s3_key(filename: str, checked_at_iso: str) -> str:
    """Build the raw S3 key using ingestion year/month folders."""
    dt = datetime.fromisoformat(checked_at_iso)
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return f"{RAW_PREFIX}/{year}/{month}/{filename}"


def upload_raw_file(bucket_name: str, s3_key: str, file_bytes: bytes) -> None:
    """Upload the raw workbook to S3."""
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_bytes,
            ContentType="application/vnd.ms-excel",
        )
        logger.info("Uploaded raw source file to s3://%s/%s", bucket_name, s3_key)
    except ClientError:
        logger.exception("Failed to upload raw source file to S3.")
        raise


# ------------------------------------------------------------
# LAMBDA HANDLER
# ------------------------------------------------------------
def lambda_handler(event, context):
    """Main entry point for the monthly fetch Lambda."""
    logger.info("FetchGasHistoryFunction invoked.")
    checked_at = utc_now_iso()

    existing_record = get_tracking_record(DATASET_NAME)

    save_tracking_record(
        existing_record,
        {
            "last_checked_at": checked_at,
            "status": "CHECKING",
        },
    )

    source_metadata = try_fetch_source_metadata(SOURCE_URL)

    previous_modified = existing_record.get("last_source_modified")
    current_modified = source_metadata.get("last_source_modified")

    if current_modified and previous_modified == current_modified:
        save_tracking_record(
            existing_record,
            {
                "last_checked_at": checked_at,
                "last_source_modified": current_modified,
                "status": "UNCHANGED",
            },
        )

        logger.info("Source unchanged based on Last-Modified header. Skipping download.")
        return {
            "status": "UNCHANGED",
            "reason": "last-modified-match",
            "checked_at": checked_at,
        }

    file_bytes = download_source_file(SOURCE_URL)

    file_hash = sha256_hex(file_bytes)
    previous_hash = existing_record.get("last_source_hash")

    if previous_hash == file_hash:
        save_tracking_record(
            existing_record,
            {
                "last_checked_at": checked_at,
                "last_source_modified": current_modified,
                "last_source_hash": file_hash,
                "status": "UNCHANGED",
            },
        )

        logger.info("Source unchanged based on SHA-256 hash. Skipping upload.")
        return {
            "status": "UNCHANGED",
            "reason": "hash-match",
            "checked_at": checked_at,
        }

    filename = SOURCE_URL.rstrip("/").split("/")[-1]
    s3_key = build_raw_s3_key(filename, checked_at)

    upload_raw_file(RAW_BUCKET_NAME, s3_key, file_bytes)

    save_tracking_record(
        existing_record,
        {
            "last_checked_at": checked_at,
            "last_downloaded_at": checked_at,
            "last_source_modified": current_modified,
            "last_source_hash": file_hash,
            "last_s3_key": s3_key,
            "status": "DOWNLOADED",
        },
    )

    logger.info(
        "New or updated source file downloaded and uploaded to s3://%s/%s",
        RAW_BUCKET_NAME,
        s3_key,
    )

    return {
        "status": "DOWNLOADED",
        "checked_at": checked_at,
        "raw_s3_key": s3_key,
    }