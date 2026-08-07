import inspect

from tenki import AsyncClient, AsyncSandbox, CommandResult


def test_supported_sdk_exposes_required_async_contract_without_api_calls():
    create_parameters = inspect.signature(AsyncClient.create).parameters
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
    assert {"env", "timeout"} <= set(start_parameters)
    assert {
        "exit_code",
        "stdout",
        "stderr",
        "signal",
        "reason",
        "errno",
    } <= result_fields
    assert inspect.iscoroutinefunction(AsyncClient.get)
    assert inspect.iscoroutinefunction(AsyncSandbox.close_if_open)
