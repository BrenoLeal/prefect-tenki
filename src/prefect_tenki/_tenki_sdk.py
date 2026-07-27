"""Small compatibility boundary around the public Tenki async SDK."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

OutputHandler = Callable[[str, bytes], None]

_COMMAND_TIMEOUT_CUSHION_SECONDS = 5

try:
    from builtins import BaseExceptionGroup
except ImportError:  # pragma: no cover - Python 3.10 only
    from exceptiongroup import BaseExceptionGroup  # type: ignore[no-redef]


class TenkiCommandResult(Protocol):
    """Subset of a Tenki command result consumed by the worker."""

    exit_code: int
    signal: str | None
    reason: str | None
    errno: int | None

    @property
    def ok(self) -> bool: ...


class TenkiSandbox(Protocol):
    """Subset of a Tenki sandbox consumed by the worker."""

    id: str


class TenkiSdk(Protocol):
    """Injectable SDK contract used to keep unit tests fully local."""

    async def create_sandbox(
        self,
        *,
        client_options: Mapping[str, str],
        sandbox_options: Mapping[str, Any],
    ) -> TenkiSandbox: ...

    async def run_command(
        self,
        sandbox: TenkiSandbox,
        *,
        command: str,
        env: Mapping[str, str],
        timeout: int,
        stream_output: bool,
        output_handler: OutputHandler | None,
    ) -> TenkiCommandResult: ...

    async def close_sandbox(self, sandbox: TenkiSandbox) -> None: ...

    async def close_sandbox_by_id(
        self,
        sandbox_id: str,
        *,
        client_options: Mapping[str, str],
    ) -> None: ...

    def is_session_not_found(self, exc: Exception) -> bool: ...


@dataclass
class _ManagedAsyncSandbox:
    """Keep the SDK client alive for exactly as long as its sandbox."""

    sandbox: Any
    client: Any

    @property
    def id(self) -> str:
        return self.sandbox.id


class AsyncTenkiSdk:
    """Adapter for ``tenki_sandbox.AsyncClient`` and ``AsyncSandbox``."""

    @staticmethod
    def _import_sdk() -> tuple[type[Any], type[Any], type[Exception]]:
        try:
            from tenki_sandbox import (
                AsyncClient,
                AsyncSandbox,
                SessionNotFoundError,
            )
        except ImportError as exc:  # pragma: no cover - packaging failure
            raise RuntimeError(
                "The Tenki SDK is not installed. Install prefect-tenki with its "
                "runtime dependencies."
            ) from exc
        return AsyncClient, AsyncSandbox, SessionNotFoundError

    async def create_sandbox(
        self,
        *,
        client_options: Mapping[str, str],
        sandbox_options: Mapping[str, Any],
    ) -> TenkiSandbox:
        client_type, _, _ = self._import_sdk()
        client = client_type(**client_options)
        try:
            sandbox = await client.create(**sandbox_options)
        except BaseException as create_error:
            try:
                await client.close()
            except BaseException as close_error:
                raise BaseExceptionGroup(
                    "Tenki sandbox creation and SDK client cleanup both failed",
                    [create_error, close_error],
                ) from None
            raise
        return _ManagedAsyncSandbox(sandbox=sandbox, client=client)

    @staticmethod
    def _unwrap_sandbox(sandbox: TenkiSandbox) -> Any:
        if isinstance(sandbox, _ManagedAsyncSandbox):
            return sandbox.sandbox
        return sandbox

    async def run_command(
        self,
        sandbox: TenkiSandbox,
        *,
        command: str,
        env: Mapping[str, str],
        timeout: int,
        stream_output: bool,
        output_handler: OutputHandler | None,
    ) -> TenkiCommandResult:
        raw_sandbox = self._unwrap_sandbox(sandbox)
        if not stream_output:
            return await raw_sandbox.exec(
                "bash",
                "-lc",
                command,
                env=dict(env),
                timeout=timeout,
            )

        process = await raw_sandbox.start(
            "bash",
            "-lc",
            command,
            env=dict(env),
            timeout=timeout,
        )
        await process.close_stdin()

        async def forward(stream_name: str, stream: Any) -> None:
            async for chunk in stream:
                if output_handler is not None:
                    output_handler(stream_name, chunk)

        stream_tasks = [
            asyncio.create_task(forward("stdout", process.stdout)),
            asyncio.create_task(forward("stderr", process.stderr)),
        ]
        try:
            # The guest enforces ``timeout``; this client-side deadline is the
            # same backstop used by the SDK's non-streaming ``exec`` helper.
            result = await process.wait(
                timeout=timeout + _COMMAND_TIMEOUT_CUSHION_SECONDS
            )
        except BaseException:
            for task in stream_tasks:
                task.cancel()
            await asyncio.gather(*stream_tasks, return_exceptions=True)
            raise
        await asyncio.gather(*stream_tasks)
        return result

    async def close_sandbox(self, sandbox: TenkiSandbox) -> None:
        raw_sandbox = self._unwrap_sandbox(sandbox)
        errors: list[BaseException] = []
        try:
            await raw_sandbox.close_if_open()
        except BaseException as exc:
            errors.append(exc)

        if isinstance(sandbox, _ManagedAsyncSandbox):
            try:
                await sandbox.client.close()
            except BaseException as exc:
                errors.append(exc)

        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(
                "Tenki sandbox and SDK client cleanup both failed",
                errors,
            )

    async def close_sandbox_by_id(
        self,
        sandbox_id: str,
        *,
        client_options: Mapping[str, str],
    ) -> None:
        client_type, _, _ = self._import_sdk()
        async with client_type(**client_options) as client:
            sandbox = await client.get(sandbox_id)
            await sandbox.close_if_open()

    def is_session_not_found(self, exc: Exception) -> bool:
        _, _, not_found_type = self._import_sdk()
        return isinstance(exc, not_found_type)
