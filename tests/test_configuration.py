from importlib.metadata import entry_points
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prefect.workers.base import BaseWorker
from pydantic import ValidationError

from prefect_tenki import TenkiCredentials
from prefect_tenki.worker import (
    TenkiWorker,
    TenkiWorkerJobConfiguration,
    TenkiWorkerVariables,
)


def credentials() -> TenkiCredentials:
    return TenkiCredentials(api_key="tk_test_secret")


def test_job_configuration_defaults_are_safe():
    configuration = TenkiWorkerJobConfiguration(credentials=credentials())

    assert configuration.cpu_cores == 2
    assert configuration.memory_mb == 4096
    assert configuration.allow_inbound is False
    assert configuration.allow_outbound is True
    assert configuration.max_duration_seconds == 3600
    assert configuration.create_timeout_seconds == 180
    assert configuration.command_timeout_seconds == 3600


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cpu_cores", 0),
        ("cpu_cores", 17),
        ("memory_mb", 127),
        ("memory_mb", 129),
        ("memory_mb", 65537),
        ("disk_size_gb", 4),
        ("disk_size_gb", 101),
        ("max_duration_seconds", 59),
        ("command_timeout_seconds", 0),
    ],
)
def test_job_configuration_validates_resource_limits(field: str, value: int):
    with pytest.raises(ValidationError):
        TenkiWorkerJobConfiguration(
            credentials=credentials(),
            **{field: value},
        )


@pytest.mark.parametrize(
    "model_type",
    [TenkiWorkerJobConfiguration, TenkiWorkerVariables],
)
def test_image_and_snapshot_are_mutually_exclusive(model_type):
    with pytest.raises(ValidationError, match="Only one of image or snapshot_id"):
        model_type(
            credentials=credentials(),
            image="ubuntu:24.04",
            snapshot_id="snap-123",
        )


def test_prepare_for_flow_run_uses_current_prefect_command_and_attribution():
    flow_run = SimpleNamespace(
        id=uuid4(),
        name="tenki-test-run",
        flow_id=uuid4(),
        deployment_id=None,
    )
    configuration = TenkiWorkerJobConfiguration(credentials=credentials())

    configuration.prepare_for_flow_run(flow_run)

    assert configuration.command == "prefect flow-run execute"
    assert configuration.name == "tenki-test-run"
    assert configuration.env["PREFECT__FLOW_RUN_ID"] == str(flow_run.id)
    assert configuration.labels["prefect.io/flow-run-id"] == str(flow_run.id)


def test_default_base_job_template_exposes_tenki_variables():
    template = TenkiWorker.get_default_base_job_template()

    assert template["job_configuration"]["credentials"] == "{{ credentials }}"
    assert template["job_configuration"]["cpu_cores"] == "{{ cpu_cores }}"
    assert "project_id" not in template["job_configuration"]
    assert "project_id" not in template["variables"]["properties"]
    assert (
        template["variables"]["properties"]["max_duration_seconds"]["default"] == 3600
    )


def test_worker_registers_type_on_import():
    assert BaseWorker.get_worker_class_from_type("tenki") is TenkiWorker


def test_package_registers_prefect_collection_entry_point():
    collections = entry_points().select(group="prefect.collections")

    assert any(
        item.name == "prefect_tenki" and item.value == "prefect_tenki"
        for item in collections
    )
