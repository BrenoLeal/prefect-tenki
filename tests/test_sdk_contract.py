import inspect

from tenki import (
    AsyncClient,
    AsyncSandbox,
    CommandResult,
    IdentityWorkspace,
    InvalidStateError,
    SessionNotFoundError,
    SessionTerminatedError,
)


def test_supported_sdk_exposes_required_async_contract_without_api_calls():
    create_parameters = inspect.signature(AsyncClient.create).parameters
    identity_workspace_parameters = inspect.signature(IdentityWorkspace).parameters
    start_parameters = inspect.signature(AsyncSandbox.start).parameters
    result_fields = set(CommandResult.__dataclass_fields__)

    assert {
        "name",
        "timeout",
        "allow_inbound",
        "allow_outbound",
        "max_duration",
        "cpu_cores",
        "memory_mb",
        "metadata",
        "snapshot_id",
        "image",
    } <= set(create_parameters)
    assert "project_id" not in create_parameters
    assert {"id", "name"} <= set(identity_workspace_parameters)
    assert {"env", "timeout"} <= set(start_parameters)
    assert {
        "exit_code",
        "stdout",
        "stderr",
        "signal",
        "reason",
        "errno",
    } <= result_fields
    assert all(
        issubclass(error_type, Exception)
        for error_type in (
            InvalidStateError,
            SessionNotFoundError,
            SessionTerminatedError,
        )
    )
    assert inspect.iscoroutinefunction(AsyncClient.get)
    assert inspect.iscoroutinefunction(AsyncSandbox.close_if_open)
