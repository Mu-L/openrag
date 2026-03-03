"""IBM Cloud Object Storage authentication and client factory."""

import os
from typing import Dict, Any

from utils.logging_config import get_logger

logger = get_logger(__name__)

# IAM auth endpoint default
_DEFAULT_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"


def create_ibm_cos_client(config: Dict[str, Any]):
    """Return an ibm_boto3 S3-compatible client.

    Supports two authentication modes:
    - IAM (preferred): IBM_COS_API_KEY + IBM_COS_SERVICE_INSTANCE_ID
    - HMAC (fallback): IBM_COS_HMAC_ACCESS_KEY_ID + IBM_COS_HMAC_SECRET_ACCESS_KEY

    Resolution order for each credential: config dict → environment variable.

    Args:
        config: Connector configuration dict with optional credential overrides.

    Returns:
        ibm_boto3 S3 client configured with the appropriate credentials.

    Raises:
        ImportError: If ibm-cos-sdk is not installed.
        ValueError: If neither IAM nor HMAC credentials can be resolved.
    """
    try:
        import ibm_boto3
        from ibm_botocore.client import Config
    except ImportError as exc:
        raise ImportError(
            "ibm-cos-sdk is required for IBM COS integration. "
            "Install it with: pip install ibm-cos-sdk"
        ) from exc

    endpoint_url = (
        config.get("endpoint_url")
        or os.getenv("IBM_COS_ENDPOINT")
    )
    if not endpoint_url:
        raise ValueError(
            "IBM COS endpoint URL is required. Set IBM_COS_ENDPOINT or provide "
            "'endpoint_url' in the connector config."
        )

    # Try IAM credentials first
    api_key = config.get("api_key") or os.getenv("IBM_COS_API_KEY")
    service_instance_id = (
        config.get("service_instance_id")
        or os.getenv("IBM_COS_SERVICE_INSTANCE_ID")
    )

    if api_key and service_instance_id:
        auth_endpoint = (
            config.get("auth_endpoint")
            or os.getenv("IBM_COS_AUTH_ENDPOINT")
            or _DEFAULT_AUTH_ENDPOINT
        )
        logger.debug("Creating IBM COS client with IAM authentication")
        return ibm_boto3.client(
            "s3",
            ibm_api_key_id=api_key,
            ibm_service_instance_id=service_instance_id,
            ibm_auth_endpoint=auth_endpoint,
            config=Config(signature_version="oauth"),
            endpoint_url=endpoint_url,
        )

    # Fall back to HMAC credentials
    hmac_access_key = (
        config.get("hmac_access_key")
        or os.getenv("IBM_COS_HMAC_ACCESS_KEY_ID")
    )
    hmac_secret_key = (
        config.get("hmac_secret_key")
        or os.getenv("IBM_COS_HMAC_SECRET_ACCESS_KEY")
    )

    if hmac_access_key and hmac_secret_key:
        logger.debug("Creating IBM COS client with HMAC authentication")
        return ibm_boto3.client(
            "s3",
            aws_access_key_id=hmac_access_key,
            aws_secret_access_key=hmac_secret_key,
            endpoint_url=endpoint_url,
        )

    raise ValueError(
        "IBM COS credentials not found. Provide either IAM credentials "
        "(IBM_COS_API_KEY + IBM_COS_SERVICE_INSTANCE_ID) or HMAC credentials "
        "(IBM_COS_HMAC_ACCESS_KEY_ID + IBM_COS_HMAC_SECRET_ACCESS_KEY)."
    )
