"""Prefect worker that executes flow runs in Tenki sandboxes."""

from __future__ import annotations

import codecs
import logging
from typing import Any

import anyio
from prefect.exceptions import InfrastructureNotFound
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field, model_validator

from prefect_tenki._tenki_sdk import (
    AsyncTenkiSdk,
    TenkiCommandResult,
    TenkiSandbox,
    TenkiSdk,
)
from prefect_tenki.credentials import TenkiCredentials

try:
    from builtins import BaseExceptionGroup
except ImportError:  # pragma: no cover - Python 3.10 only
    from exceptiongroup import BaseExceptionGroup  # type: ignore[no-redef]


def _validate_image_source(image: str | None, snapshot_id: str | None) -> None:
    if image and snapshot_id:
        raise ValueError("Only one of image or snapshot_id may be configured.")


class TenkiWorkerJobConfiguration(BaseJobConfiguration):
    """Configuration used to create one Tenki sandbox per flow run."""

    credentials: TenkiCredentials = Field(
        default=...,
        description="Credentials used by the worker to call the Tenki API.",
    )
    workspace_id: str | None = Field(default=None)
    cpu_cores: int = Field(default=2, ge=1, le=16)
    memory_mb: int = Field(default=4096, ge=128, le=65536, multiple_of=2)
    disk_size_gb: int | None = Field(default=None, ge=5, le=100)
    image: str | None = Field(
        default=None,
        description="Optional Tenki image containing Prefect and flow dependencies.",
    )
    snapshot_id: str | None = Field(
        default=None,
        description="Optional Tenki snapshot used instead of an image.",
    )
    allow_inbound: bool = Field(default=False)
    allow_outbound: bool = Field(
        default=True,
        description="Allow the flow-run process to reach Prefect and code sources.",
    )
    max_duration_seconds: int = Field(default=3600, ge=60)
    create_timeout_seconds: int = Field(default=180, ge=1)
    command_timeout_seconds: int = Field(default=3600, ge=1)
    stream_output: bool = Field(default=True)
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_sources(self) -> TenkiWorkerJobConfiguration:
        _validate_image_source(self.image, self.snapshot_id)
        return self


class TenkiWorkerVariables(BaseVariables):
    """User-facing variables for a Tenki work pool."""

    credentials: TenkiCredentials = Field(default=...)
    workspace_id: str | None = Field(default=None)
    cpu_cores: int = Field(default=2, ge=1, le=16)
    memory_mb: int = Field(default=4096, ge=128, le=65536, multiple_of=2)
    disk_size_gb: int | None = Field(default=None, ge=5, le=100)
    image: str | None = Field(default=None)
    snapshot_id: str | None = Field(default=None)
    allow_inbound: bool = Field(default=False)
    allow_outbound: bool = Field(default=True)
    max_duration_seconds: int = Field(default=3600, ge=60)
    create_timeout_seconds: int = Field(default=180, ge=1)
    command_timeout_seconds: int = Field(default=3600, ge=1)
    stream_output: bool = Field(default=True)
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_sources(self) -> TenkiWorkerVariables:
        _validate_image_source(self.image, self.snapshot_id)
        return self


class TenkiWorkerResult(BaseWorkerResult):
    """Final infrastructure result returned to Prefect."""


class _Utf8OutputLogger:
    """Decode split UTF-8 chunks and emit complete lines without losing bytes."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._decoders = {
            "stdout": codecs.getincrementaldecoder("utf-8")(errors="replace"),
            "stderr": codecs.getincrementaldecoder("utf-8")(errors="replace"),
        }
        self._buffers = {"stdout": "", "stderr": ""}

    def __call__(self, stream_name: str, chunk: bytes) -> None:
        decoder = self._decoders[stream_name]
        self._buffers[stream_name] += decoder.decode(chunk)
        self._emit_complete_lines(stream_name)

    def _emit_complete_lines(self, stream_name: str) -> None:
        buffer = self._buffers[stream_name]
        lines = buffer.splitlines(keepends=True)
        self._buffers[stream_name] = ""
        for line in lines:
            if line.endswith(("\n", "\r")):
                self._emit(stream_name, line.rstrip("\r\n"))
            else:
                self._buffers[stream_name] = line

    def flush(self) -> None:
        for stream_name, decoder in self._decoders.items():
            self._buffers[stream_name] += decoder.decode(b"", final=True)
            if self._buffers[stream_name]:
                self._emit(stream_name, self._buffers[stream_name])
                self._buffers[stream_name] = ""

    def _emit(self, stream_name: str, text: str) -> None:
        if text:
            self._logger.info("Tenki %s: %s", stream_name, text)


class TenkiWorker(
    BaseWorker[
        TenkiWorkerJobConfiguration,
        TenkiWorkerVariables,
        TenkiWorkerResult,
    ]
):
    """Run Prefect flow runs in short-lived Tenki sandboxes."""

    type = "tenki"
    job_configuration = TenkiWorkerJobConfiguration
    job_configuration_variables = TenkiWorkerVariables
    _display_name = "Tenki"
    _description = "Execute flow runs in isolated, ephemeral Tenki sandboxes."
    _documentation_url = "https://tenki.cloud/docs/sandbox/sdk"

    def __init__(self, *args: Any, sdk: TenkiSdk | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._tenki_sdk: TenkiSdk = sdk or AsyncTenkiSdk()

    @staticmethod
    def _sandbox_options(
        configuration: TenkiWorkerJobConfiguration,
    ) -> dict[str, Any]:
        options: dict[str, Any] = {
            "name": configuration.name,
            "workspace_id": configuration.workspace_id,
            "cpu_cores": configuration.cpu_cores,
            "memory_mb": configuration.memory_mb,
            "disk_size_gb": configuration.disk_size_gb,
            "image": configuration.image,
            "snapshot_id": configuration.snapshot_id,
            "allow_inbound": configuration.allow_inbound,
            "allow_outbound": configuration.allow_outbound,
            "max_duration": configuration.max_duration_seconds,
            "timeout": configuration.create_timeout_seconds,
            "wait": True,
            "metadata": {
                **configuration.metadata,
                **configuration.labels,
                "prefect.io/worker-type": "tenki",
            },
        }
        return {key: value for key, value in options.items() if value is not None}

    @staticmethod
    def _status_code(result: TenkiCommandResult) -> int:
        if result.ok:
            return 0
        return result.exit_code if result.exit_code != 0 else 1

    async def run(
        self,
        flow_run: Any,
        configuration: TenkiWorkerJobConfiguration,
        task_status: anyio.abc.TaskStatus[str] | None = None,
    ) -> TenkiWorkerResult:
        """Create a sandbox, execute the prepared command, and always close it."""
        del flow_run  # The prepared configuration already contains run attribution.
        if not configuration.command:
            raise ValueError("The Tenki job configuration has no command to execute.")

        sandbox: TenkiSandbox | None = None
        primary_error: BaseException | None = None
        try:
            # A cancellation must not lose a sandbox created by an in-flight request.
            with anyio.CancelScope(shield=True):
                sandbox = await self._tenki_sdk.create_sandbox(
                    client_options=configuration.credentials.get_client_options(),
                    sandbox_options=self._sandbox_options(configuration),
                )

            if task_status is not None:
                task_status.started(sandbox.id)

            output_logger = _Utf8OutputLogger(self._logger)
            try:
                result = await self._tenki_sdk.run_command(
                    sandbox,
                    command=configuration.command,
                    env={
                        key: value
                        for key, value in configuration.env.items()
                        if value is not None
                    },
                    timeout=configuration.command_timeout_seconds,
                    stream_output=configuration.stream_output,
                    output_handler=(
                        output_logger if configuration.stream_output else None
                    ),
                )
            finally:
                output_logger.flush()

            status_code = self._status_code(result)
            if status_code != 0:
                self._logger.error(
                    "Tenki command failed: exit_code=%s signal=%s reason=%s errno=%s",
                    result.exit_code,
                    result.signal,
                    result.reason,
                    result.errno,
                )
            return TenkiWorkerResult(identifier=sandbox.id, status_code=status_code)
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            if sandbox is not None:
                try:
                    with anyio.CancelScope(shield=True):
                        await self._tenki_sdk.close_sandbox(sandbox)
                except BaseException as cleanup_error:
                    if primary_error is not None:
                        raise BaseExceptionGroup(
                            "Tenki flow-run execution and sandbox cleanup both failed",
                            [primary_error, cleanup_error],
                        ) from None
                    raise

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: TenkiWorkerJobConfiguration,
        grace_seconds: int = 30,
    ) -> None:
        """Terminate a Tenki sandbox by its Prefect infrastructure identifier."""
        del grace_seconds  # Tenki owns the termination grace policy.
        try:
            with anyio.CancelScope(shield=True):
                await self._tenki_sdk.close_sandbox_by_id(
                    infrastructure_pid,
                    client_options=configuration.credentials.get_client_options(),
                )
        except Exception as exc:
            if self._tenki_sdk.is_session_not_found(exc):
                raise InfrastructureNotFound(
                    f"Tenki sandbox {infrastructure_pid!r} was not found."
                ) from exc
            raise
