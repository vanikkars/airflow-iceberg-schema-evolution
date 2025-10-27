"""
Vault handler for reading secrets from HashiCorp Vault.
"""

import logging
import hvac

logger = logging.getLogger(__name__)


class VaultHandler:
    """Lightweight Vault client for reading secrets."""

    def __init__(self, vault_addr, vault_token):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self._client = None

    def get_client(self):
        """Get or create Vault client."""
        if self._client is None:
            self._client = hvac.Client(
                url=self.vault_addr,
                token=self.vault_token
            )
            if not self._client.is_authenticated():
                raise Exception("Failed to authenticate with Vault")
            logger.info(f"Authenticated with Vault at {self.vault_addr}")
        return self._client

    def get_secret(self, path):
        """
        Read a secret from Vault KV v2 engine.

        Args:
            path: Secret path (e.g., 'secret/postgres/ecommerce')

        Returns:
            Dictionary of secret data
        """
        client = self.get_client()
        try:
            # For KV v2, the path needs to be formatted with /data/
            secret_path_parts = path.split('/', 1)
            mount_point = secret_path_parts[0]
            secret_path = secret_path_parts[1] if len(secret_path_parts) > 1 else ''

            response = client.secrets.kv.v2.read_secret_version(
                path=secret_path,
                mount_point=mount_point
            )
            logger.info(f"Successfully read secret from {path}")
            return response['data']['data']
        except Exception as e:
            logger.error(f"Failed to read secret from {path}: {e}")
            raise
