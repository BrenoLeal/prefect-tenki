"""Opt-in live smoke test for the Prefect Tenki worker.

This module is not collected by pytest. Running it directly may consume Tenki
credits by creating one short-lived sandbox.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import anyio
from tenki_sandbox import AsyncClient

from prefect_tenki import (
    TenkiCredentials,
    TenkiWorker,
    TenkiWorkerJobConfiguration,
)

_MAX_HOLD_SECONDS = 120


class _ConsoleTaskStatus:
    def started(self, value: str) -> None:
        print(f"Sandbox is running: {value}", flush=True)
        print("Open the Tenki dashboard now to inspect it.", flush=True)


def _required_api_key() -> str:
    api_key = os.getenv("TENKI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "TENKI_API_KEY is required. Set it only in your local shell; "
            "never commit it."
        )
    return api_key


def _hold_seconds() -> int:
    raw_value = os.getenv("TENKI_SMOKE_HOLD_SECONDS", "30")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError("TENKI_SMOKE_HOLD_SECONDS must be an integer.") from exc
    if not 0 <= value <= _MAX_HOLD_SECONDS:
        raise RuntimeError(
            f"TENKI_SMOKE_HOLD_SECONDS must be between 0 and {_MAX_HOLD_SECONDS}."
        )
    return value


def _identity_only() -> bool:
    return os.getenv("TENKI_SMOKE_IDENTITY_ONLY", "").lower() in {
        "1",
        "true",
        "yes",
    }


def _select_scope(
    identity: Any,
    requested_workspace_id: str | None,
    requested_project_id: str | None,
) -> tuple[str | None, str | None]:
    workspaces = list(identity.workspaces)
    print("Accessible Tenki scope:")
    for workspace in workspaces:
        print(f"- workspace: {workspace.name} ({workspace.id})")
        for project in workspace.projects:
            print(f"  - project: {project.name} ({project.id})")

    if requested_workspace_id:
        workspaces = [
            workspace
            for workspace in workspaces
            if workspace.id == requested_workspace_id
        ]
        if not workspaces:
            raise RuntimeError(
                "TENKI_WORKSPACE_ID is not accessible with this API key."
            )

    projects = [
        (workspace, project)
        for workspace in workspaces
        for project in workspace.projects
    ]
    if requested_project_id:
        matches = [
            (workspace, project)
            for workspace, project in projects
            if project.id == requested_project_id
        ]
        if not matches:
            raise RuntimeError(
                "TENKI_PROJECT_ID is not accessible with this API key/workspace."
            )
        workspace, project = matches[0]
        return project.id, workspace.id

    if len(projects) == 1:
        workspace, project = projects[0]
        print(f"Auto-selected project: {project.name} ({project.id})")
        return project.id, workspace.id

    if len(projects) > 1:
        raise RuntimeError(
            "More than one Tenki project is accessible. Set TENKI_PROJECT_ID "
            "to choose one before creating a sandbox."
        )

    workspace_id = workspaces[0].id if len(workspaces) == 1 else None
    return None, workspace_id


async def _discover_scope(
    credentials: TenkiCredentials,
) -> tuple[str | None, str | None]:
    async with AsyncClient(**credentials.get_client_options()) as client:
        identity = await client.who_am_i()
    return _select_scope(
        identity,
        os.getenv("TENKI_WORKSPACE_ID") or None,
        os.getenv("TENKI_PROJECT_ID") or None,
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    endpoint = os.getenv("TENKI_API_ENDPOINT") or os.getenv("TENKI_API_URL")
    credentials = TenkiCredentials(
        api_key=_required_api_key(),
        api_endpoint=endpoint,
    )
    project_id, workspace_id = await _discover_scope(credentials)
    if _identity_only():
        print("Identity-only check complete; no sandbox was created.")
        return

    hold_seconds = _hold_seconds()
    command = (
        "set -eu; "
        'echo "prefect-tenki live smoke"; '
        'printf "hostname="; hostname; '
        'printf "user="; whoami; '
        "python3 --version; "
        f"sleep {hold_seconds}; "
        'echo "smoke complete"'
    )
    configuration = TenkiWorkerJobConfiguration(
        name="prefect-tenki-live-smoke",
        command=command,
        credentials=credentials,
        project_id=project_id,
        workspace_id=workspace_id,
        cpu_cores=1,
        memory_mb=512,
        allow_inbound=False,
        allow_outbound=False,
        max_duration_seconds=max(120, hold_seconds + 90),
        create_timeout_seconds=180,
        command_timeout_seconds=max(60, hold_seconds + 30),
        stream_output=True,
        metadata={"source": "prefect-tenki-live-smoke"},
    )

    worker = TenkiWorker(work_pool_name="prefect-tenki-live-smoke")
    result = await worker.run(
        object(),
        configuration,
        task_status=_ConsoleTaskStatus(),
    )
    if result.status_code != 0:
        raise RuntimeError(
            f"Sandbox command failed with status {result.status_code} "
            f"(sandbox {result.identifier})."
        )
    print(f"Smoke test passed and sandbox {result.identifier} was closed.")


if __name__ == "__main__":
    anyio.run(main)
