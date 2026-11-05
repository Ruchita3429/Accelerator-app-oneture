"""Shared support for optional AWS STS session credentials."""


def boto3_credentials(config: dict) -> dict:
    """Return credential kwargs for boto3 without exposing their values."""
    result = {
        "aws_access_key_id": config.get("access_key"),
        "aws_secret_access_key": config.get("secret_key"),
    }
    if config.get("session_token"):
        result["aws_session_token"] = config["session_token"]
    return result


def redshift_copy_credentials(config: dict) -> str:
    """Build Redshift COPY credential syntax, including STS when needed."""
    result = (
        f"ACCESS_KEY_ID '{config['access_key']}' "
        f"SECRET_ACCESS_KEY '{config['secret_key']}'"
    )
    if config.get("session_token"):
        result += f" SESSION_TOKEN '{config['session_token']}'"
    return result
