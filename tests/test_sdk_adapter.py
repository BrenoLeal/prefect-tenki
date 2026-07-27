from __future__ import annotations

import asyncio
from typing import Any

import anyio
import pytest

from prefect_tenki._tenki_sdk import AsyncTenkiSdk

try:
    from builtins import BaseExceptionGroup
except ImportError:  # pragma: no cover - Python 3.10 only
    from exceptiongroup import BaseExceptionGroup  # type: ignore[no-redef]


class RawSandbox:
    def __init__(self, sandbox_id: str = "sb-managed") -> None:
        self.id = sandbox_id
        self.closed = False

    async def close_if_open(self) -> None:
        self.closed = True


class ChunkStream:
    def __init__(self, *chunks: bytes) -> None:
        self._chunks = iter(chunks)

    def __aiter__(self) -> ChunkStream:
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self._chunks)
        except StopIteration:
            raise StopAsyncIteration from None


class BlockingStream:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = False

    def __aiter__(self) -> BlockingStream:
        return self

    async def __anext__(self) -> bytes:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        raise StopAsyncIteration


class FakeProcess:
    def __init__(
        self,
        *,
        stdout: Any,
        stderr: Any,
        result: Any = None,
        wait_error: BaseException | None = None,
    ) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.result = result
        self.wait_error = wait_error
        self.stdin_closed = False
        self.wait_timeout: int | None = None

    async def close_stdin(self) -> None:
        self.stdin_closed = True

    async def wait(self, timeout: int | None = None) -> Any:
        self.wait_timeout = timeout
        if self.wait_error is not None:
            for stream in (self.stdout, self.stderr):
                started = getattr(stream, "started", None)
                if started is not None:
                    await started.wait()
            raise self.wait_error
        return self.result


class CommandSandbox:
    def __init__(self, *, process: FakeProcess | None = None, result: Any = None):
        self.id = "sb-command"
        self.process = process
        self.result = result
        self.start_call: tuple[tuple[Any, ...], dict[str, Any]] | None = None
        self.exec_call: tuple[tuple[Any, ...], dict[str, Any]] | None = None

    async def start(self, *args: Any, **kwargs: Any) -> FakeProcess:
        self.start_call = (args, kwargs)
        assert self.process is not None
        return self.process

    async def exec(self, *args: Any, **kwargs: Any) -> Any:
        self.exec_call = (args, kwargs)
        return self.result


class FakeAsyncClient:
    instances: list[FakeAsyncClient] = []
    create_error: Exception | None = None

    def __init__(self, **options: Any) -> None:
        self.options = options
        self.closed = False
        self.sandbox = RawSandbox()
        self.close_error: Exception | None = None
        self.__class__.instances.append(self)

    async def create(self, **options: Any) -> RawSandbox:
        self.create_options = options
        if self.__class__.create_error:
            raise self.__class__.create_error
        return self.sandbox

    async def get(self, sandbox_id: str) -> RawSandbox:
        self.sandbox = RawSandbox(sandbox_id)
        return self.sandbox

    async def close(self) -> None:
        self.closed = True
        if self.close_error:
            raise self.close_error

    async def __aenter__(self) -> FakeAsyncClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


@pytest.fixture(autouse=True)
def reset_client_state(monkeypatch):
    FakeAsyncClient.instances = []
    FakeAsyncClient.create_error = None
    monkeypatch.setattr(
        AsyncTenkiSdk,
        "_import_sdk",
        staticmethod(lambda: (FakeAsyncClient, object, LookupError)),
    )


def test_adapter_closes_sandbox_and_owning_client():
    async def scenario() -> None:
        adapter = AsyncTenkiSdk()
        managed = await adapter.create_sandbox(
            client_options={"auth_token": "secret"},
            sandbox_options={"name": "test"},
        )

        assert managed.id == "sb-managed"
        client = FakeAsyncClient.instances[0]
        assert client.closed is False

        await adapter.close_sandbox(managed)

        assert client.sandbox.closed is True
        assert client.closed is True

    anyio.run(scenario)


def test_adapter_closes_client_when_create_fails():
    async def scenario() -> None:
        FakeAsyncClient.create_error = RuntimeError("create failed")
        adapter = AsyncTenkiSdk()

        with pytest.raises(RuntimeError, match="create failed"):
            await adapter.create_sandbox(
                client_options={"auth_token": "secret"},
                sandbox_options={"name": "test"},
            )

        assert FakeAsyncClient.instances[0].closed is True

    anyio.run(scenario)


def test_adapter_preserves_create_and_client_cleanup_failures():
    async def scenario() -> None:
        FakeAsyncClient.create_error = RuntimeError("create failed")
        adapter = AsyncTenkiSdk()

        original_init = FakeAsyncClient.__init__

        def init_with_close_failure(self, **options):
            original_init(self, **options)
            self.close_error = OSError("client close failed")

        FakeAsyncClient.__init__ = init_with_close_failure
        try:
            with pytest.raises(BaseExceptionGroup) as exc_info:
                await adapter.create_sandbox(
                    client_options={"auth_token": "secret"},
                    sandbox_options={"name": "test"},
                )
        finally:
            FakeAsyncClient.__init__ = original_init

        assert [str(error) for error in exc_info.value.exceptions] == [
            "create failed",
            "client close failed",
        ]

    anyio.run(scenario)


def test_adapter_close_by_id_closes_sandbox_and_client():
    async def scenario() -> None:
        adapter = AsyncTenkiSdk()

        await adapter.close_sandbox_by_id(
            "sb-remote",
            client_options={"auth_token": "secret"},
        )

        client = FakeAsyncClient.instances[0]
        assert client.sandbox.id == "sb-remote"
        assert client.sandbox.closed is True
        assert client.closed is True

    anyio.run(scenario)


def test_adapter_runs_non_streaming_command_with_timeout():
    async def scenario() -> None:
        expected = object()
        sandbox = CommandSandbox(result=expected)

        result = await AsyncTenkiSdk().run_command(
            sandbox,
            command="echo hello",
            env={"GREETING": "hello"},
            timeout=7,
            stream_output=False,
            output_handler=None,
        )

        assert result is expected
        assert sandbox.exec_call == (
            ("bash", "-lc", "echo hello"),
            {"env": {"GREETING": "hello"}, "timeout": 7},
        )

    anyio.run(scenario)


def test_adapter_streams_output_with_local_timeout_backstop():
    async def scenario() -> None:
        expected = object()
        process = FakeProcess(
            stdout=ChunkStream(b"out-1", b"out-2"),
            stderr=ChunkStream(b"err-1"),
            result=expected,
        )
        sandbox = CommandSandbox(process=process)
        output: list[tuple[str, bytes]] = []

        result = await AsyncTenkiSdk().run_command(
            sandbox,
            command="echo hello",
            env={"GREETING": "hello"},
            timeout=7,
            stream_output=True,
            output_handler=lambda stream, chunk: output.append((stream, chunk)),
        )

        assert result is expected
        assert sandbox.start_call == (
            ("bash", "-lc", "echo hello"),
            {"env": {"GREETING": "hello"}, "timeout": 7},
        )
        assert process.stdin_closed is True
        assert process.wait_timeout == 12
        assert sorted(output) == [
            ("stderr", b"err-1"),
            ("stdout", b"out-1"),
            ("stdout", b"out-2"),
        ]

    anyio.run(scenario)


def test_adapter_cancels_stream_forwarders_when_wait_times_out():
    async def scenario() -> None:
        stdout = BlockingStream()
        stderr = BlockingStream()
        process = FakeProcess(
            stdout=stdout,
            stderr=stderr,
            wait_error=TimeoutError("timed out waiting for command"),
        )
        sandbox = CommandSandbox(process=process)

        with pytest.raises(TimeoutError, match="timed out waiting for command"):
            await AsyncTenkiSdk().run_command(
                sandbox,
                command="sleep 60",
                env={},
                timeout=7,
                stream_output=True,
                output_handler=None,
            )

        assert process.wait_timeout == 12
        assert stdout.cancelled is True
        assert stderr.cancelled is True

    anyio.run(scenario)
