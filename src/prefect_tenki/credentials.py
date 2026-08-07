"""Credential blocks used by the Tenki worker."""

from prefect.blocks.abstract import CredentialsBlock
from pydantic import Field, SecretStr, field_validator


class TenkiCredentials(CredentialsBlock):
    """Credentials used to authenticate with the Tenki Sandbox API."""

    _block_type_name = "Tenki Credentials"
    _documentation_url = "https://tenki.cloud/docs/sandbox/sdk"

    api_key: SecretStr = Field(
        default=...,
        title="API Key",
        description="Tenki API key used only by the worker process.",
    )
    api_endpoint: str | None = Field(
        default=None,
        title="API Endpoint",
        description=(
            "Optional Tenki API endpoint. Leave unset to use the SDK default."
        ),
    )

    @field_validator("api_endpoint")
    @classmethod
    def _normalize_api_endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().rstrip("/")
        return normalized or None

    def get_client_options(self) -> dict[str, str]:
        """Return explicit SDK options without exposing them in the guest env."""
        options = {"auth_token": self.api_key.get_secret_value()}
        if self.api_endpoint:
            options["base_url"] = self.api_endpoint
        return options

    def get_client(self):
        """Return a native asynchronous Tenki client."""
        from tenki import AsyncClient

        return AsyncClient(**self.get_client_options())
