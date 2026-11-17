import boto3
from urllib.parse import urlparse


def start_sap_glue_job(
    glue_job_name,
    sap_config,
    aws_access_key,
    aws_secret_key,
    s3_base_path,
    region="ap-south-1",
    page_size=1000,
    load_type="FULL",
    write_mode="overwrite",
    output_format="parquet",
    aws_session_token=None,
):
    """Start the SAP OData-to-S3 Glue job using credentials supplied by the UI."""

    required_sap_fields = (
        "token_url",
        "client_id",
        "client_secret",
        "odata_base_url",
        "entity_set",
    )
    missing_fields = [
        field for field in required_sap_fields
        if sap_config.get(field) is None or not str(sap_config.get(field)).strip()
    ]
    if missing_fields:
        raise ValueError("Missing SAP configuration: " + ", ".join(missing_fields))

    # Metadata discovery uses this temporary value; it must never be migrated.
    if sap_config["entity_set"].strip() == "__DISCOVERY__":
        raise ValueError("Select a real SAP OData entity before starting migration.")

    if not glue_job_name or not glue_job_name.strip():
        raise ValueError("AWS Glue Job Name is required.")
    if not aws_access_key or not aws_access_key.strip():
        raise ValueError("AWS Access Key ID is required.")
    if not aws_secret_key or not aws_secret_key.strip():
        raise ValueError("AWS Secret Access Key is required.")
    if not region or not region.strip():
        raise ValueError("AWS Region is required.")

    parsed_s3_path = urlparse((s3_base_path or "").strip())
    if parsed_s3_path.scheme != "s3" or not parsed_s3_path.netloc:
        raise ValueError("S3 destination must be a valid s3://bucket[/prefix] path.")

    page_size = int(page_size)
    if page_size <= 0:
        raise ValueError("Page size must be greater than zero.")

    load_type = load_type.upper()
    if load_type not in ("FULL", "INCREMENTAL"):
        raise ValueError("Load type must be FULL or INCREMENTAL.")

    write_mode = write_mode.lower()
    output_format = output_format.lower()

    glue_client = boto3.client(
        "glue",
        region_name=region.strip(),
        aws_access_key_id=aws_access_key.strip(),
        aws_secret_access_key=aws_secret_key.strip(),
        **({"aws_session_token": aws_session_token.strip()} if aws_session_token and aws_session_token.strip() else {}),
    )

    # These names must match the arguments resolved by the existing Glue script.
    job_arguments = {
        "--s3_base_path": s3_base_path.strip().rstrip("/"),
        "--token_url": sap_config["token_url"].strip(),
        "--client_id": sap_config["client_id"].strip(),
        "--client_secret": sap_config["client_secret"],
        "--odata_base_url": sap_config["odata_base_url"].strip().rstrip("/"),
        "--entity_set": sap_config["entity_set"].strip(),
        "--region": region.strip(),
        "--page_size": str(page_size),
        "--load_type": load_type,
        "--write_mode": write_mode,
        "--output_format": output_format,
    }

    response = glue_client.start_job_run(
        JobName=glue_job_name.strip(),
        Arguments=job_arguments,
    )
    return response["JobRunId"]


def get_sap_glue_job_status(
    glue_job_name,
    job_run_id,
    aws_access_key,
    aws_secret_key,
    region="ap-south-1",
    aws_session_token=None,
):
    if not glue_job_name or not job_run_id:
        raise ValueError("Glue job name and job run ID are required to check status.")
    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials are required to check Glue job status.")

    glue_client = boto3.client(
        "glue",
        region_name=region,
        aws_access_key_id=aws_access_key.strip(),
        aws_secret_access_key=aws_secret_key.strip(),
        **({"aws_session_token": aws_session_token.strip()} if aws_session_token and aws_session_token.strip() else {}),
    )
    response = glue_client.get_job_run(
        JobName=glue_job_name.strip(),
        RunId=job_run_id,
        PredecessorsIncluded=False,
    )
    job_run = response["JobRun"]
    return {
        "job_run_id": job_run_id,
        "status": job_run["JobRunState"],
        "started_on": job_run.get("StartedOn"),
        "completed_on": job_run.get("CompletedOn"),
        "error_message": job_run.get("ErrorMessage"),
        "execution_time": job_run.get("ExecutionTime"),
        "dpu_seconds": job_run.get("DPUSeconds"),
        "worker_type": job_run.get("WorkerType"),
        "number_of_workers": job_run.get("NumberOfWorkers"),
        "max_capacity": job_run.get("MaxCapacity"),
        "execution_class": job_run.get("ExecutionClass"),
    }
