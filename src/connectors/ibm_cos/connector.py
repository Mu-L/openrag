"""IBM Cloud Object Storage connector for OpenRAG."""

import mimetypes
import os
from datetime import datetime, timezone
from posixpath import basename
from typing import Any, Dict, List, Optional

from connectors.base import BaseConnector, ConnectorDocument, DocumentACL
from utils.logging_config import get_logger

from .auth import create_ibm_cos_client

logger = get_logger(__name__)

# Separator used in composite file IDs: "<bucket>::<key>"
_ID_SEPARATOR = "::"


def _make_file_id(bucket: str, key: str) -> str:
    return f"{bucket}{_ID_SEPARATOR}{key}"


def _split_file_id(file_id: str):
    """Split a composite file ID into (bucket, key). Raises ValueError if invalid."""
    if _ID_SEPARATOR not in file_id:
        raise ValueError(f"Invalid IBM COS file ID (missing separator): {file_id!r}")
    bucket, key = file_id.split(_ID_SEPARATOR, 1)
    return bucket, key


class IBMCOSConnector(BaseConnector):
    """Connector for IBM Cloud Object Storage.

    Supports IAM (API key) and HMAC credential modes. Credentials are read
    from the connector config dict first, then from environment variables.

    Config dict keys:
        bucket_names (list[str]): Buckets to ingest from. Required.
        prefix (str): Optional object key prefix filter.
        endpoint_url (str): Overrides IBM_COS_ENDPOINT.
        api_key (str): Overrides IBM_COS_API_KEY.
        service_instance_id (str): Overrides IBM_COS_SERVICE_INSTANCE_ID.
        hmac_access_key (str): HMAC mode – overrides IBM_COS_HMAC_ACCESS_KEY_ID.
        hmac_secret_key (str): HMAC mode – overrides IBM_COS_HMAC_SECRET_ACCESS_KEY.
        connection_id (str): Connection identifier used for logging.
    """

    CONNECTOR_NAME = "IBM Cloud Object Storage"
    CONNECTOR_DESCRIPTION = "Add knowledge from IBM Cloud Object Storage"
    CONNECTOR_ICON = "ibm-cos"

    # BaseConnector uses these to check env-var availability for IAM mode.
    # HMAC-only setups will show as "unavailable" in the UI but can still be
    # used when credentials are supplied in the config dict directly.
    CLIENT_ID_ENV_VAR = "IBM_COS_API_KEY"
    CLIENT_SECRET_ENV_VAR = "IBM_COS_SERVICE_INSTANCE_ID"

    def __init__(self, config: Dict[str, Any]):
        if config is None:
            config = {}
        super().__init__(config)

        self.bucket_names: List[str] = config.get("bucket_names") or []
        self.prefix: str = config.get("prefix", "")
        self.connection_id: str = config.get("connection_id", "default")

        # Resolved service instance ID used as ACL owner fallback
        self._service_instance_id: str = (
            config.get("service_instance_id")
            or os.getenv("IBM_COS_SERVICE_INSTANCE_ID", "")
        )

        self._client = None  # Lazy-initialised in authenticate()

    def _get_client(self):
        """Return (and cache) the IBM COS boto3-compatible client."""
        if self._client is None:
            self._client = create_ibm_cos_client(self.config)
        return self._client

    # ------------------------------------------------------------------
    # BaseConnector abstract method implementations
    # ------------------------------------------------------------------

    async def authenticate(self) -> bool:
        """Validate credentials by listing buckets on the COS service."""
        try:
            client = self._get_client()
            client.list_buckets()
            self._authenticated = True
            logger.debug(f"IBM COS authenticated for connection {self.connection_id}")
            return True
        except Exception as exc:
            logger.warning(f"IBM COS authentication failed: {exc}")
            self._authenticated = False
            return False

    async def list_files(
        self,
        page_token: Optional[str] = None,
        max_files: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """List objects across all configured buckets.

        Returns:
            dict with keys:
                "files": list of file dicts (id, name, bucket, size, modified_time)
                "next_page_token": continuation token or None
        """
        client = self._get_client()
        files: List[Dict[str, Any]] = []

        # Page token format: "<bucket_index>:<s3_continuation_token>"
        start_bucket_index = 0
        s3_continuation_token: Optional[str] = None

        if page_token:
            try:
                idx_str, cos_token = page_token.split(":", 1)
                start_bucket_index = int(idx_str)
                s3_continuation_token = cos_token or None
            except (ValueError, AttributeError):
                logger.warning(f"Ignoring invalid page_token: {page_token!r}")

        next_page_token: Optional[str] = None

        for bucket_index, bucket in enumerate(self.bucket_names):
            if bucket_index < start_bucket_index:
                continue

            # Reset S3 token when we move to a new bucket
            if bucket_index > start_bucket_index:
                s3_continuation_token = None

            list_kwargs: Dict[str, Any] = {"Bucket": bucket}
            if self.prefix:
                list_kwargs["Prefix"] = self.prefix
            if s3_continuation_token:
                list_kwargs["ContinuationToken"] = s3_continuation_token

            try:
                while True:
                    response = client.list_objects_v2(**list_kwargs)
                    for obj in response.get("Contents", []):
                        key = obj["Key"]
                        # Skip "directory" placeholder keys
                        if key.endswith("/"):
                            continue
                        files.append(
                            {
                                "id": _make_file_id(bucket, key),
                                "name": basename(key) or key,
                                "bucket": bucket,
                                "key": key,
                                "size": obj.get("Size", 0),
                                "modified_time": obj.get("LastModified", "").isoformat()
                                if obj.get("LastModified")
                                else None,
                            }
                        )

                        if max_files and len(files) >= max_files:
                            # Emit a page token pointing at the current position
                            if response.get("IsTruncated"):
                                next_page_token = (
                                    f"{bucket_index}:{response['NextContinuationToken']}"
                                )
                            return {"files": files, "next_page_token": next_page_token}

                    if response.get("IsTruncated"):
                        list_kwargs["ContinuationToken"] = response[
                            "NextContinuationToken"
                        ]
                    else:
                        break

            except Exception as exc:
                logger.error(f"Failed to list objects in bucket {bucket!r}: {exc}")
                continue

        return {"files": files, "next_page_token": next_page_token}

    async def get_file_content(self, file_id: str) -> ConnectorDocument:
        """Download an object from IBM COS and return a ConnectorDocument.

        Args:
            file_id: Composite ID in the form "<bucket>::<key>".

        Returns:
            ConnectorDocument with content bytes, ACL, and metadata.
        """
        bucket, key = _split_file_id(file_id)
        client = self._get_client()

        # Download object
        response = client.get_object(Bucket=bucket, Key=key)
        content: bytes = response["Body"].read()

        # Object metadata
        head = client.head_object(Bucket=bucket, Key=key)
        last_modified: datetime = head.get("LastModified") or datetime.now(timezone.utc)
        size: int = head.get("ContentLength", len(content))

        # MIME type detection: content-type header → filename extension fallback
        mime_type: str = (
            head.get("ContentType")
            or mimetypes.guess_type(key)[0]
            or "application/octet-stream"
        )

        filename = basename(key) or key

        acl = await self._extract_acl(bucket, key)

        return ConnectorDocument(
            id=file_id,
            filename=filename,
            mimetype=mime_type,
            content=content,
            source_url=f"cos://{bucket}/{key}",
            acl=acl,
            modified_time=last_modified,
            created_time=last_modified,  # IBM COS does not expose creation time
            metadata={
                "ibm_cos_bucket": bucket,
                "ibm_cos_key": key,
                "size": size,
            },
        )

    async def _extract_acl(self, bucket: str, key: str) -> DocumentACL:
        """Fetch object ACL from IBM COS and map it to DocumentACL.

        Falls back to a minimal ACL (owner = service instance ID) on failure.
        """
        try:
            client = self._get_client()
            acl_response = client.get_object_acl(Bucket=bucket, Key=key)

            owner_id: str = (
                acl_response.get("Owner", {}).get("DisplayName")
                or acl_response.get("Owner", {}).get("ID")
                or self._service_instance_id
            )

            allowed_users: List[str] = []
            for grant in acl_response.get("Grants", []):
                grantee = grant.get("Grantee", {})
                permission = grant.get("Permission", "")
                if permission in ("FULL_CONTROL", "READ"):
                    user_id = (
                        grantee.get("DisplayName")
                        or grantee.get("ID")
                        or grantee.get("EmailAddress")
                    )
                    if user_id and user_id not in allowed_users:
                        allowed_users.append(user_id)

            return DocumentACL(
                owner=owner_id,
                allowed_users=allowed_users,
                allowed_groups=[],
            )
        except Exception as exc:
            logger.warning(
                f"Could not fetch ACL for cos://{bucket}/{key}: {exc}. "
                "Using fallback ACL."
            )
            return DocumentACL(
                owner=self._service_instance_id or None,
                allowed_users=[],
                allowed_groups=[],
            )

    # ------------------------------------------------------------------
    # Webhook / subscription (stub — IBM COS events require IBM Event
    # Notifications service; not in scope for this connector version)
    # ------------------------------------------------------------------

    async def setup_subscription(self) -> str:
        """No-op: IBM COS event notifications are out of scope for this connector."""
        return ""

    async def handle_webhook(self, payload: Dict[str, Any]) -> List[str]:
        """No-op: webhooks are not supported in this connector version."""
        return []

    def extract_webhook_channel_id(
        self, payload: Dict[str, Any], headers: Dict[str, str]
    ) -> Optional[str]:
        return None

    async def cleanup_subscription(self, subscription_id: str) -> bool:
        """No-op: no subscription to clean up."""
        return True
