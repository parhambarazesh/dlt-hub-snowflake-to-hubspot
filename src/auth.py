"""JWT key-pair authentication for Snowflake SQL REST API.

Generates a JWT using the RSA private key (rsa_key.p8) with a SHA-256 fingerprint-based
issuer claim, as required by Snowflake's key-pair authentication.
"""

import hashlib
import os
import time
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from requests.auth import AuthBase


class SnowflakeJWTAuth(AuthBase):
    """requests AuthBase that attaches a Snowflake key-pair JWT to each request.

    The token is cached and regenerated when it is within 60 seconds of expiry.
    """

    TOKEN_LIFETIME_SECONDS = 3600  # 1 hour
    RENEWAL_BUFFER_SECONDS = 60

    def __init__(
        self,
        account: str,
        user: str,
        private_key_path: str | os.PathLike | None = None,
    ):
        # Snowflake account identifiers must be uppercase and use the
        # account-locator form (no region suffix on the *account* part, but
        # the full account identifier as given by Snowflake is fine).
        self._account = account.upper().split(".")[0]
        self._user = user.upper()
        self._private_key = self._load_private_key(private_key_path)
        self._fingerprint = self._compute_fingerprint()

        # cached token state
        self._token: str | None = None
        self._token_expiry: float = 0.0

    # ------------------------------------------------------------------
    # Key handling
    # ------------------------------------------------------------------

    @staticmethod
    def _load_private_key(path: str | os.PathLike | None):
        if path is None:
            path = Path(__file__).parent / "rsa_key.p8"
        with open(path, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)

    def _compute_fingerprint(self) -> str:
        """SHA-256 fingerprint of the *public* key in DER format."""
        pub_der = self._private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        digest = hashlib.sha256(pub_der).digest()
        import base64

        return base64.b64encode(digest).decode("utf-8")

    # ------------------------------------------------------------------
    # Token generation
    # ------------------------------------------------------------------

    def _generate_token(self) -> str:
        now = int(time.time())
        qualified_user = f"{self._account}.{self._user}"
        payload = {
            "iss": f"{qualified_user}.SHA256:{self._fingerprint}",
            "sub": qualified_user,
            "iat": now,
            "exp": now + self.TOKEN_LIFETIME_SECONDS,
        }
        token = jwt.encode(
            payload,
            self._private_key,
            algorithm="RS256",
        )
        self._token_expiry = now + self.TOKEN_LIFETIME_SECONDS
        return token

    @property
    def token(self) -> str:
        if (
            self._token is None
            or time.time() >= self._token_expiry - self.RENEWAL_BUFFER_SECONDS
        ):
            self._token = self._generate_token()
        return self._token

    # ------------------------------------------------------------------
    # requests integration
    # ------------------------------------------------------------------

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token}"
        r.headers["X-Snowflake-Authorization-Token-Type"] = "KEYPAIR_JWT"
        return r
