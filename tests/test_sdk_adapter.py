from __future__ import annotations

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
