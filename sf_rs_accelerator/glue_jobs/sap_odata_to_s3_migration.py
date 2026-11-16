import sys
import json
import time
import base64
import urllib.parse
import urllib.request
import urllib.error
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

# -----------------------------------------------------------------------------
# AWS Glue Job Configuration
# -----------------------------------------------------------------------------

REQUIRED = [
    "JOB_NAME",
    "s3_base_path",
    "token_url",
    "client_id",
    "client_secret",
    "odata_base_url",
    "entity_set",
]

OPTIONAL_KEYS = [
    "region",
    "page_size",
    "load_type",
    "write_mode",
    "output_format",
]

# Identify optional parameters supplied at runtime
present = [
    key for key in OPTIONAL_KEYS
    if f"--{key}" in sys.argv
]

# Retrieve Glue job parameters
args = getResolvedOptions(
    sys.argv,
    REQUIRED + present
)

# -----------------------------------------------------------------------------
# Configuration Variables
# -----------------------------------------------------------------------------

TOKEN_URL = args["token_url"]

CLIENT_ID = args["client_id"]

CLIENT_SECRET = args["client_secret"]

ODATA_BASE_URL = args["odata_base_url"].rstrip("/")

REGION = args.get("region", "ap-south-1")

ENTITY_SET = args["entity_set"]

PAGE_SIZE = int(args.get("page_size", "1000"))

LOAD_TYPE = args.get("load_type", "FULL").upper()

WRITE_MODE = args.get("write_mode", "overwrite")

OUTPUT_FORMAT = args.get("output_format", "parquet").lower()

S3_BASE_PATH = args["s3_base_path"].rstrip("/")

# Watermark configuration
WATERMARK_KEY = f"metadata/{ENTITY_SET}_watermark.json"

# This allows our Glue script to interact with Amazon S3 programmatically.
s3_client = boto3.client("s3", region_name=REGION)

parsed_s3_path = urllib.parse.urlparse(S3_BASE_PATH)

S3_BUCKET = parsed_s3_path.netloc

# -----------------------------------------------------------------------------
# Read Previous Watermark from S3
# -----------------------------------------------------------------------------


def get_previous_watermark():

    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=WATERMARK_KEY)

        watermark_data = json.loads(response["Body"].read().decode("utf-8"))

        previous_watermark = watermark_data.get("last_successful_watermark")

        print(f"Previous watermark: {previous_watermark}")

        return previous_watermark

    except ClientError as error:

        error_code = error.response["Error"]["Code"]

        if error_code in ("NoSuchKey", "404"):
            print("Watermark file not found. First load detected.")
            return None

        raise


# Validate configuration parameters

if PAGE_SIZE <= 0:
    raise ValueError("PAGE_SIZE must be greater than 0.")

if OUTPUT_FORMAT not in ("parquet", "json", "csv", "orc"):
    raise ValueError(f"Unsupported output format: {OUTPUT_FORMAT}")

if WRITE_MODE not in ("overwrite", "append"):
    raise ValueError(f"Unsupported write mode: {WRITE_MODE}")

# -----------------------------------------------------------------------------
# Initialize Spark and AWS Glue
# -----------------------------------------------------------------------------

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args["JOB_NAME"], args)

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# -----------------------------------------------------------------------------
# OAuth 2.0 Authentication
# -----------------------------------------------------------------------------


def get_access_token(max_retries=3):  # Sends the authentication request

    # Encode Client ID and Client Secret for HTTP Basic Authentication
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"

    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("ascii")

    # OAuth request payload
    payload = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode(
        "utf-8"
    )

    # Prepare HTTP request
    request = urllib.request.Request(
        TOKEN_URL,
        data=payload,
        headers={
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )

    # Execute request with retry handling
    for attempt in range(max_retries + 1):

        try:
            with urllib.request.urlopen(request, timeout=30) as response:

                token_response = json.loads(response.read().decode("utf-8"))

            access_token = token_response["access_token"]

            print("SAP OAuth authentication successful.")

            return access_token

        except urllib.error.HTTPError as error:

            response_body = error.read().decode("utf-8", errors="replace")[:500]

            if error.code not in (429, 500, 502, 503, 504):
                raise Exception(
                    f"OAuth authentication failed: "
                    f"HTTP {error.code}: {response_body}"
                )

            if attempt == max_retries:
                raise Exception(
                    f"OAuth authentication failed after "
                    f"{max_retries} retries: HTTP {error.code}"
                )

        except (urllib.error.URLError, TimeoutError, OSError) as error:

            if attempt == max_retries:
                raise Exception(
                    f"OAuth network error after " f"{max_retries} retries: {error}"
                )

        delay = min(30, 2 ** (attempt + 1))

        print(f"OAuth request failed. " f"Retrying in {delay} seconds...")

        time.sleep(delay)


# -----------------------------------------------------------------------------
# Fetch one page from SAP OData V2
# -----------------------------------------------------------------------------


def read_odata_page(access_token, offset, max_retries=5):

    query = urllib.parse.urlencode(
        {"$top": PAGE_SIZE, "$skip": offset, "$orderby": "ID asc"}
    )

    entity_url = f"{ODATA_BASE_URL}/{ENTITY_SET}"

    url = f"{entity_url}?{query}"

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/atom+xml",
            "Authorization": f"Bearer {access_token}",
        },
    )

    for attempt in range(max_retries + 1):

        try:
            with urllib.request.urlopen(request, timeout=120) as response:

                xml_response = response.read().decode("utf-8")

            root = ET.fromstring(xml_response)

            # Extract individual records from the Atom XML feed
            entries = root.findall("{http://www.w3.org/2005/Atom}entry")

            rows = []

            for entry in entries:

                properties = entry.find(
                    ".//{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties"
                )

                if properties is None:
                    continue

                record = {prop.tag.split("}")[-1]: prop.text for prop in properties}

                rows.append(record)

            return rows

        except urllib.error.HTTPError as error:

            if error.code not in (429, 500, 502, 503, 504):
                raise Exception(f"OData request failed: HTTP {error.code}")

            if attempt == max_retries:
                raise Exception(
                    f"OData request failed after retries: HTTP {error.code}"
                )

        except (urllib.error.URLError, TimeoutError, OSError) as error:

            if attempt == max_retries:
                raise Exception(f"OData connection failed after retries: {error}")

        delay = min(60, 2 ** (attempt + 1))

        print(f"OData request failed. " f"Retrying in {delay} seconds...")

        time.sleep(delay)

    return []


# -----------------------------------------------------------------------------
# Fetch all records using pagination
# -----------------------------------------------------------------------------


def fetch_all_odata_records(access_token):

    all_rows = []

    offset = 0

    while True:

        print(f"Fetching records: offset={offset}, " f"page_size={PAGE_SIZE}")

        rows = read_odata_page(access_token, offset)

        page_count = len(rows)

        if page_count == 0:
            break

        all_rows.extend(rows)

        print(f"Retrieved {page_count} records. " f"Total records: {len(all_rows)}")

        offset += page_count

        if page_count < PAGE_SIZE:
            break

    print(f"Total records extracted: {len(all_rows)}")

    return all_rows


# -----------------------------------------------------------------------------
# Filter Records Based on Watermark
# -----------------------------------------------------------------------------


def filter_incremental_records(records, previous_watermark):

    # First load: Process all records
    if previous_watermark is None:
        print("No previous watermark. Processing all records.")
        return records

    # Convert previous watermark into datetime
    watermark_datetime = datetime.strptime(previous_watermark, "%Y-%m-%d %H:%M")

    # Subtract 5 minutes to create an overlap window
    overlap_datetime = watermark_datetime - timedelta(minutes=5)

    filtered_records = []

    for record in records:

        timestamp = record.get("UPDATED_AT")

        if not timestamp:
            continue

        record_datetime = datetime.strptime(timestamp.strip(), "%m/%d/%Y %H:%M")

        if record_datetime >= overlap_datetime:
            filtered_records.append(record)

    print(f"Previous watermark: {watermark_datetime}")
    print(f"Overlap watermark: {overlap_datetime}")
    print(f"Records before filtering: {len(records)}")
    print(f"Records after filtering: {len(filtered_records)}")

    return filtered_records


# -----------------------------------------------------------------------------
# Update Watermark in S3
# -----------------------------------------------------------------------------


def update_watermark(records, previous_watermark):

    if not records:
        print("No records available to update watermark.")
        return

    # Find the latest timestamp among processed records
    latest_timestamp = max(
        datetime.strptime(record["UPDATED_AT"].strip(), "%m/%d/%Y %H:%M")
        for record in records
        if record.get("UPDATED_AT")
    )

    # Preserve the previous watermark only during incremental loading
    if LOAD_TYPE == "INCREMENTAL" and previous_watermark:

        previous_datetime = datetime.strptime(
            previous_watermark, "%Y-%m-%d %H:%M"
        )

        latest_timestamp = max(latest_timestamp, previous_datetime)

    watermark_data = {
        "entity_set": ENTITY_SET,
        "last_successful_watermark": latest_timestamp.strftime("%Y-%m-%d %H:%M"),
    }

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps(watermark_data, indent=4),
        ContentType="application/json",
    )

    print(f"Watermark updated successfully: {watermark_data}")

# -----------------------------------------------------------------------------
# Write extracted OData records to Amazon S3 in Parquet format
# -----------------------------------------------------------------------------


def write_to_s3(records):

    if not records:
        raise Exception("No records available to write to S3.")

    # Convert Python dictionaries into a Spark DataFrame
    json_records = [json.dumps(record) for record in records]

    sdf = spark.read.json(sc.parallelize(json_records))

    # Remove OData metadata if present
    if "__metadata" in sdf.columns:
        sdf = sdf.drop("__metadata")

    # Add extraction date for S3 partitioning
    sdf = sdf.withColumn("load_date", F.current_date())

    # Add batch ID to identify records from this execution
    sdf = sdf.withColumn("batch_id", F.lit(batch_id))

    # Deduplicate records during incremental loading

    if LOAD_TYPE == "INCREMENTAL":

        print("Reading existing records from S3...")

        output_path = f"{S3_BASE_PATH}/{ENTITY_SET}/"

        existing_df = spark.read.format(OUTPUT_FORMAT).load(output_path)

        # Combine existing records with newly extracted records
        sdf = existing_df.unionByName(sdf, allowMissingColumns=True)

        # Keep the latest version of each ID
        window_spec = Window.partitionBy("ID").orderBy(
            F.to_timestamp(F.col("UPDATED_AT"), "M/d/yyyy H:mm").desc(),
            F.col("batch_id").desc(),
        )

        sdf = (
            sdf.withColumn("row_number", F.row_number().over(window_spec))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
        )

        print("Duplicate records removed successfully.")

    # Count records belonging to the current batch after deduplication
    expected_count = sdf.filter(F.col("batch_id") == batch_id).count()

    print(f"Records expected in current batch: {expected_count}")

    # Control the number of output files
    rows_per_file = 500000

    target_files = max(1, (expected_count + rows_per_file - 1) // rows_per_file)

    sdf = sdf.coalesce(target_files)

    # Convert Spark DataFrame into Glue DynamicFrame
    dyf = DynamicFrame.fromDF(sdf, glueContext, f"SAP_OData_{ENTITY_SET}")

    # Evaluate basic data quality
    EvaluateDataQuality().process_rows(
        frame=dyf,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": f"EvaluateDataQuality_{ENTITY_SET}",
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL",
        },
    )

    # Define S3 output location
    output_path = f"{S3_BASE_PATH}/{ENTITY_SET}/"

    # Write Parquet files to S3
    (
        sdf.write.mode("overwrite")
        .option("compression", "snappy")
        .format(OUTPUT_FORMAT)
        .partitionBy("load_date")
        .save(output_path)
    )

    print(f"Successfully wrote {expected_count} records to {output_path}")

    return expected_count


# -----------------------------------------------------------------------------
# Source-to-Target Record Count Validation
# -----------------------------------------------------------------------------


def validate_record_count(expected_count):

    output_path = f"{S3_BASE_PATH}/{ENTITY_SET}/"

    # Read the data written to S3
    target_df = spark.read.format(OUTPUT_FORMAT).load(output_path)

    # Filter records belonging to the current Glue execution
    current_batch_df = target_df.filter(F.col("batch_id") == batch_id)

    actual_count = current_batch_df.count()

    print(f"Expected record count: {expected_count}")
    print(f"Actual record count: {actual_count}")

    if expected_count != actual_count:
        raise Exception(
            f"Record count validation failed! "
            f"Expected: {expected_count}, Actual: {actual_count}"
        )

    print("Source-to-target record count validation successful.")

    return actual_count


# -----------------------------------------------------------------------------
# Main Execution - SAP OData to S3
# -----------------------------------------------------------------------------

print("=" * 60)
print("Starting SAP OData to S3 Migration")
print("=" * 60)

print(f"Entity Set : {ENTITY_SET}")
print(f"Load Type  : {LOAD_TYPE}")
print(f"Page Size  : {PAGE_SIZE}")
print(f"Destination: {S3_BASE_PATH}")

# Generate a unique ID for this Glue execution
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

print(f"Batch ID: {batch_id}")

# Step 0: Read previous watermark from S3
print("\nStep 0: Reading previous watermark...")

previous_watermark = get_previous_watermark()

# Step 1: Authenticate with SAP
print("\nStep 1: Authenticating with SAP...")

access_token = get_access_token()

# Step 2: Extract records from SAP OData
print("\nStep 2: Extracting SAP records...")

records = fetch_all_odata_records(access_token)

source_count = len(records)

print(f"Total source records extracted: {source_count}")

# Apply filtering based on load type
if LOAD_TYPE == "INCREMENTAL":

    if previous_watermark is None:
        raise Exception(
            "No previous watermark found. "
            "Please run a FULL load before an INCREMENTAL load."
        )

    print("\nApplying incremental filtering...")

    records = filter_incremental_records(records, previous_watermark)

    WRITE_MODE = "overwrite"

elif LOAD_TYPE == "FULL":

    print("\nFull load selected. Processing all records.")

    WRITE_MODE = "overwrite"

else:
    raise ValueError(f"Invalid LOAD_TYPE: {LOAD_TYPE}")

filtered_count = len(records)

print(f"Records to load: {filtered_count}")
print(f"S3 write mode: {WRITE_MODE}")


# Step 3: Write extracted records to S3

if filtered_count == 0:

    print("No new records found. Skipping S3 write.")
    print("Watermark remains unchanged.")

else:

    print("\nStep 3: Loading records into Amazon S3...")

    expected_count = write_to_s3(records)

    # Step 4: Validate source-to-target record counts
    print("\nStep 4: Validating record counts...")

    validate_record_count(expected_count)

    # Step 5: Update watermark after successful validation
    print("\nStep 5: Updating watermark...")

    update_watermark(records, previous_watermark)

# Step 4: Complete Glue job
print("\nStep 4: Migration completed successfully.")

print(f"Total records loaded: {filtered_count}")

job.commit()
