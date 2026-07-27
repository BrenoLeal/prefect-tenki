from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import anyio
import pytest
from prefect.exceptions import InfrastructureNotFound

from prefect_tenki import TenkiCredentials
from prefect_tenki.worker import TenkiWorker, TenkiWorkerJobConfiguration

try:
    from builtins import BaseExceptionGroup
except ImportError:  # pragma: no cover - Python 3.10 only
    from exceptiongroup import BaseExceptionGroup  # type: ignore[no-redef]


@dataclass
class FakeResult:
    exit_code: int = 0
    signal: str | None = None
    reason: str | None = None
    errno: int | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.signal


@dataclass
class FakeSandbox:
    id: str = "sb-test-123"


class SessionMissingError(Exception):
    pass


class CleanupError(Exception):
    pass


class FakeSdk:
    def __init__(self, result: FakeResult | None = None) -> None:
        self.result = result or FakeResult()
        self.sandbox = FakeSandbox()
        self.create_error: Exception | None = None
        self.run_error: Exception | None = None
        self.close_error: Exception | None = None
        self.close_by_id_error: Exception | None = None
        self.created_client_options: dict[str, str] | None = None
        self.created_sandbox_options: dict[str, Any] | None = None
        self.run_options: dict[str, Any] | None = None
        self.closed: list[str] = []
        self.closed_by_id: list[str] = []

    async def create_sandbox(self, *, client_options, sandbox_options):
        self.created_client_options = dict(client_options)
        self.created_sandbox_options = dict(sandbox_options)
        if self.create_error:
            raise self.create_error
        return self.sandbox

    async def run_command(
        self,
        sandbox,
        *,
        command,
        env,
        timeout,
        stream_output,
        output_handler,
    ):
        self.run_options = {
            "sandbox": sandbox,
            "command": command,
            "env": dict(env),
            "timeout": timeout,
            "stream_output": stream_output,
        }
        if self.run_error:
            raise self.run_error
        if output_handler:
            output_handler("stdout", b"caf\xc3")
            output_handler("stdout", b"\xa9\n")
            output_handler("stderr", b"diagnostic\n")
        return self.result

    async def close_sandbox(self, sandbox):
        self.closed.append(sandbox.id)
        if self.close_error:
            raise self.close_error

    async def close_sandbox_by_id(self, sandbox_id, *, client_options):
        self.closed_by_id.append(sandbox_id)
        if self.close_by_id_error:
            raise self.close_by_id_error

    def is_session_not_found(self, exc: Exception) -> bool:
        return isinstance(exc, SessionMissingError)


class TaskStatus:
    def __init__(self) -> None:
        self.value: str | None = None

    def started(self, value: str) -> None:
        self.value = value


def configuration(**overrides: Any) -> TenkiWorkerJobConfiguration:
    values: dict[str, Any] = {
        "credentials": TenkiCredentials(
            api_key="tk_worker_secret",
            api_endpoint="https://api.example.test",
        ),
        "command": "prefect flow-run execute",
        "name": "tenki-flow-run",
        "env": {
            "PREFECT_API_URL": "https://api.prefect.cloud/api/accounts/test",
            "PREFECT_API_KEY": "prefect-secret",
            "UNSET": None,
        },
        "labels": {"prefect.io/flow-run-id": "flow-run-123"},
        "metadata": {"purpose": "unit-test"},
    }
    values.update(overrides)
    return TenkiWorkerJobConfiguration(**values)


def run_worker(
    worker: TenkiWorker, config: TenkiWorkerJobConfiguration, task_status=None
):
    return anyio.run(worker.run, object(), config, task_status)


def test_success_reports_sandbox_id_forwards_env_and_always_closes(caplog):
    sdk = FakeSdk()
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)
    task_status = TaskStatus()
    caplog.set_level(logging.INFO)

    result = run_worker(worker, configuration(), task_status)

    assert result.identifier == "sb-test-123"
    assert result.status_code == 0
    assert task_status.value == "sb-test-123"
    assert sdk.closed == ["sb-test-123"]
    assert sdk.created_client_options == {
        "auth_token": "tk_worker_secret",
        "base_url": "https://api.example.test",
    }
    assert "auth_token" not in sdk.created_sandbox_options
    assert sdk.created_sandbox_options["max_duration"] == 3600
    assert sdk.created_sandbox_options["wait"] is True
    assert "wait_for_runtime" not in sdk.created_sandbox_options
    assert sdk.created_sandbox_options["allow_inbound"] is False
    assert sdk.created_sandbox_options["allow_outbound"] is True
    assert sdk.created_sandbox_options["metadata"] == {
        "purpose": "unit-test",
        "prefect.io/flow-run-id": "flow-run-123",
        "prefect.io/worker-type": "tenki",
    }
    assert sdk.run_options["env"] == {
        "PREFECT_API_URL": "https://api.prefect.cloud/api/accounts/test",
        "PREFECT_API_KEY": "prefect-secret",
    }
    assert "café" in caplog.text
    assert "diagnostic" in caplog.text
    assert "tk_worker_secret" not in caplog.text
    assert "prefect-secret" not in caplog.text


def test_signal_with_zero_exit_code_is_not_success():
    sdk = FakeSdk(FakeResult(exit_code=0, signal="SIGKILL", reason="killed"))
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    result = run_worker(worker, configuration(stream_output=False))

    assert result.status_code == 1
    assert sdk.closed == ["sb-test-123"]


def test_nonzero_exit_code_is_preserved():
    sdk = FakeSdk(FakeResult(exit_code=7, reason="exit"))
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    result = run_worker(worker, configuration(stream_output=False))

    assert result.status_code == 7


def test_create_failure_does_not_attempt_cleanup_without_a_handle():
    sdk = FakeSdk()
    sdk.create_error = RuntimeError("create failed")
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    with pytest.raises(RuntimeError, match="create failed"):
        run_worker(worker, configuration())

    assert sdk.closed == []


def test_missing_prepared_command_fails_before_creating_sandbox():
    sdk = FakeSdk()
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    with pytest.raises(ValueError, match="no command"):
        run_worker(worker, configuration(command=None))

    assert sdk.created_sandbox_options is None


def test_execution_failure_still_closes_sandbox():
    sdk = FakeSdk()
    sdk.run_error = RuntimeError("execution failed")
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    with pytest.raises(RuntimeError, match="execution failed"):
        run_worker(worker, configuration())

    assert sdk.closed == ["sb-test-123"]


def test_cleanup_failure_is_visible_after_successful_execution():
    sdk = FakeSdk()
    sdk.close_error = CleanupError("cleanup failed")
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    with pytest.raises(CleanupError, match="cleanup failed"):
        run_worker(worker, configuration())


def test_primary_and_cleanup_failures_are_both_preserved():
    sdk = FakeSdk()
    sdk.run_error = RuntimeError("execution failed")
    sdk.close_error = CleanupError("cleanup failed")
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    with pytest.raises(BaseExceptionGroup) as exc_info:
        run_worker(worker, configuration())

    messages = [str(error) for error in exc_info.value.exceptions]
    assert messages == ["execution failed", "cleanup failed"]


def test_kill_infrastructure_closes_sandbox_by_id():
    sdk = FakeSdk()
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    anyio.run(worker.kill_infrastructure, "sb-remote-123", configuration())

    assert sdk.closed_by_id == ["sb-remote-123"]


def test_kill_infrastructure_maps_missing_session():
    sdk = FakeSdk()
    sdk.close_by_id_error = SessionMissingError("missing")
    worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

    with pytest.raises(InfrastructureNotFound, match="sb-missing"):
        anyio.run(worker.kill_infrastructure, "sb-missing", configuration())


def test_cancellation_during_create_keeps_handle_and_closes_sandbox():
    async def scenario() -> None:
        create_started = anyio.Event()
        release_create = anyio.Event()

        class BlockingCreateSdk(FakeSdk):
            async def create_sandbox(self, *, client_options, sandbox_options):
                create_started.set()
                await release_create.wait()
                return await super().create_sandbox(
                    client_options=client_options,
                    sandbox_options=sandbox_options,
                )

            async def run_command(self, *args, **kwargs):
                await anyio.sleep(0)
                return await super().run_command(*args, **kwargs)

        sdk = BlockingCreateSdk()
        worker = TenkiWorker(work_pool_name="tenki-test", sdk=sdk)

        async def invoke_worker() -> None:
            await worker.run(object(), configuration())

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(invoke_worker)
            await create_started.wait()
            task_group.cancel_scope.cancel()
            release_create.set()

        assert sdk.closed == ["sb-test-123"]

    anyio.run(scenario)
