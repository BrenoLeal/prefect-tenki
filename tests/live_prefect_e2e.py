"""Opt-in Prefect + Tenki end-to-end validation.

This script starts an ephemeral authenticated Prefect Server, exposes it through
a temporary Cloudflare tunnel, starts the real Tenki worker, and submits the
Iris example flow. It may consume Tenki credits and is never collected by
pytest or executed by CI.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import hashlib
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

REPOSITORY_URL = "https://github.com/BrenoLeal/prefect-tenki.git"
DEFAULT_SOURCE_BRANCH = "feat/prefect-tenki-worker"
PREFECT_IMAGE = "prefecthq/prefect-client:3.8.0-python3.12"
REMOTE_DATASET_PATH = "/tmp/prefect-tenki-e2e/iris.csv"
REMOTE_MANIFEST_PATH = "/tmp/prefect-tenki-e2e/manifest.json"
TUNNEL_URL_PATTERN = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


def _parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Required acknowledgement that this run creates a paid sandbox.",
    )
    parser.add_argument("--hold-seconds", type=int, default=180)
    parser.add_argument("--port", type=int, default=4200)
    parser.add_argument("--source-branch", default=DEFAULT_SOURCE_BRANCH)
    parser.add_argument(
        "--cloudflared",
        type=Path,
        default=repo_root / "build" / "tools" / "cloudflared.exe",
    )
    return parser.parse_args()


def _load_local_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} must be set in .env.local or the environment.")
    return value


def _prefect_cli() -> Path:
    executable = Path(sys.executable)
    candidate = executable.with_name("prefect.exe" if os.name == "nt" else "prefect")
    if not candidate.exists():
        raise RuntimeError(f"Prefect CLI was not found next to {executable}.")
    return candidate


def _start_process(
    command: list[str],
    *,
    env: dict[str, str],
    log_path: Path,
) -> tuple[subprocess.Popen[Any], Any]:
    log_handle = log_path.open("w", encoding="utf-8")
    creation_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = subprocess.Popen(
        command,
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        creationflags=creation_flags,
    )
    return process, log_handle


def _stop_process(process: subprocess.Popen[Any] | None, log_handle: Any) -> None:
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    if log_handle is not None:
        log_handle.close()


def _authorization_header(auth_string: str) -> str:
    token = base64.b64encode(auth_string.encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _wait_for_http(url: str, auth_string: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        request = urllib.request.Request(
            url,
            headers={"Authorization": _authorization_header(auth_string)},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.HTTPError) as exc:
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {url}: {last_error}")


def _wait_for_tunnel_url(
    process: subprocess.Popen[Any],
    log_path: Path,
    timeout: int = 90,
) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"cloudflared exited with status {process.returncode}; "
                f"inspect {log_path}."
            )
        if log_path.exists():
            match = TUNNEL_URL_PATTERN.search(
                log_path.read_text(encoding="utf-8", errors="replace")
            )
            if match:
                return match.group(0)
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for the tunnel URL in {log_path}.")


async def _create_credentials_and_pool(
    *,
    api_key: str,
    api_endpoint: str | None,
    pool_name: str,
) -> UUID:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate

    from prefect_tenki import TenkiCredentials, TenkiWorker

    credentials = TenkiCredentials(
        api_key=api_key,
        api_endpoint=api_endpoint,
    )
    async with get_client() as client:
        block_id = await credentials.save(
            "tenki-e2e",
            overwrite=True,
            client=client,
        )
        await client.create_work_pool(
            WorkPoolCreate(
                name=pool_name,
                type=TenkiWorker.type,
                base_job_template=TenkiWorker.get_default_base_job_template(),
            ),
            overwrite=True,
        )
    return block_id


def _create_deployment(
    *,
    block_id: UUID,
    pool_name: str,
    deployment_name: str,
    public_api_url: str,
    auth_string: str,
    source_branch: str,
    hold_seconds: int,
    project_id: str | None,
    workspace_id: str | None,
) -> UUID:
    from prefect.flows import Flow
    from prefect.runner.storage import GitRepository

    remote_flow = Flow.from_source(
        source=GitRepository(
            url=REPOSITORY_URL,
            branch=source_branch,
            pull_interval=None,
        ),
        entrypoint="examples/iris_dataset_flow.py:tenki_iris_e2e",
    )
    return remote_flow.deploy(
        name=deployment_name,
        work_pool_name=pool_name,
        build=False,
        push=False,
        print_next_steps=False,
        parameters={"hold_seconds": hold_seconds},
        tags=["prefect-tenki", "live-e2e"],
        job_variables={
            "credentials": {
                "$ref": {"block_document_id": str(block_id)},
            },
            "project_id": project_id,
            "workspace_id": workspace_id,
            "cpu_cores": 1,
            "memory_mb": 1024,
            "image": PREFECT_IMAGE,
            "allow_inbound": False,
            "allow_outbound": True,
            "max_duration_seconds": max(360, hold_seconds + 180),
            "create_timeout_seconds": 240,
            "command_timeout_seconds": max(300, hold_seconds + 120),
            "stream_output": True,
            "metadata": {
                "source": "prefect-tenki-live-e2e",
                "dataset": "uci-iris",
            },
            "env": {
                "PREFECT_API_URL": public_api_url,
                "PREFECT_API_AUTH_STRING": auth_string,
            },
        },
    )


async def _create_flow_run(deployment_id: UUID, hold_seconds: int) -> UUID:
    from prefect.client.orchestration import get_client

    async with get_client() as client:
        flow_run = await client.create_flow_run_from_deployment(
            deployment_id,
            parameters={"hold_seconds": hold_seconds},
            tags=["prefect-tenki", "live-e2e"],
        )
    return flow_run.id


def _validate_download(dataset_path: Path, manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset_bytes = dataset_path.read_bytes()
    digest = hashlib.sha256(dataset_bytes).hexdigest()
    with dataset_path.open(encoding="utf-8", newline="") as dataset_file:
        rows = list(csv.reader(dataset_file))
    data_rows = len(rows) - 1
    if data_rows != 150:
        raise RuntimeError(f"Downloaded dataset has {data_rows} rows, expected 150.")
    if digest != manifest["sha256"]:
        raise RuntimeError("Downloaded dataset SHA-256 does not match the manifest.")
    return {
        "rows": data_rows,
        "bytes": len(dataset_bytes),
        "sha256": digest,
        "sandbox_hostname": manifest["sandbox_hostname"],
        "source_url": manifest["source_url"],
    }


async def _monitor_and_download(
    *,
    flow_run_id: UUID,
    api_key: str,
    api_endpoint: str | None,
    artifact_dir: Path,
    timeout: int,
    report: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    from prefect.client.orchestration import get_client
    from tenki_sandbox import (
        AsyncClient,
        SessionNotFoundError,
        SessionTerminatedError,
    )
    from tenki_sandbox import FileNotFoundError as TenkiFileNotFoundError

    client_options: dict[str, str] = {"auth_token": api_key}
    if api_endpoint:
        client_options["base_url"] = api_endpoint

    deadline = time.monotonic() + timeout
    sandbox_id: str | None = None
    sandbox: Any = None
    evidence: dict[str, Any] | None = None
    final_state: str | None = None
    dataset_path = artifact_dir / "iris.csv"
    manifest_path = artifact_dir / "manifest.json"

    async with (
        get_client() as prefect_client,
        AsyncClient(**client_options) as tenki_client,
    ):
        while time.monotonic() < deadline:
            flow_run = await prefect_client.read_flow_run(flow_run_id)
            if flow_run.infrastructure_pid and sandbox_id is None:
                sandbox_id = flow_run.infrastructure_pid
                report["sandbox_id"] = sandbox_id
                print(
                    f"Sandbox Tenki ativo: {sandbox_id}. "
                    "Abra a Tenki UI para observá-lo por três minutos.",
                    flush=True,
                )
                sandbox = await tenki_client.get(sandbox_id)

            if sandbox is not None and evidence is None:
                manifest_tmp = manifest_path.with_suffix(".json.tmp")
                dataset_tmp = dataset_path.with_suffix(".csv.tmp")
                try:
                    await sandbox.fs.stat(REMOTE_MANIFEST_PATH)
                    await sandbox.fs.download(
                        REMOTE_MANIFEST_PATH,
                        manifest_tmp,
                    )
                    await sandbox.fs.download(
                        REMOTE_DATASET_PATH,
                        dataset_tmp,
                    )
                except TenkiFileNotFoundError:
                    manifest_tmp.unlink(missing_ok=True)
                    dataset_tmp.unlink(missing_ok=True)
                else:
                    manifest_tmp.replace(manifest_path)
                    dataset_tmp.replace(dataset_path)
                    evidence = _validate_download(dataset_path, manifest_path)
                    report["dataset"] = evidence
                    print(
                        f"Dataset recuperado do sandbox: {dataset_path} "
                        f"({evidence['rows']} rows).",
                        flush=True,
                    )

            if flow_run.state and flow_run.state.is_final():
                final_state = flow_run.state.name
                break
            await asyncio.sleep(2)

        if final_state is None:
            raise RuntimeError("Timed out waiting for the Prefect flow run.")
        if evidence is None:
            raise RuntimeError(
                f"Flow reached {final_state} before its dataset was downloaded."
            )

        if sandbox_id:
            cleanup_deadline = time.monotonic() + 45
            while time.monotonic() < cleanup_deadline:
                try:
                    current = await tenki_client.get(sandbox_id)
                except (SessionNotFoundError, SessionTerminatedError):
                    report["sandbox_cleanup"] = "closed"
                    break
                if current.state in {"TERMINATING", "TERMINATED"}:
                    report["sandbox_cleanup"] = current.state.lower()
                    break
                await asyncio.sleep(2)
            else:
                report["sandbox_cleanup"] = "not-confirmed"

    assert sandbox_id is not None
    return final_state, evidence


async def _emergency_close(
    sandbox_id: str,
    api_key: str,
    api_endpoint: str | None,
) -> None:
    from tenki_sandbox import AsyncClient, SessionNotFoundError

    client_options: dict[str, str] = {"auth_token": api_key}
    if api_endpoint:
        client_options["base_url"] = api_endpoint
    try:
        async with AsyncClient(**client_options) as client:
            sandbox = await client.get(sandbox_id)
            await sandbox.close_if_open()
    except SessionNotFoundError:
        return


def _sanitize_logs(paths: list[Path], secrets_to_remove: list[str]) -> None:
    for path in paths:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for secret_value in secrets_to_remove:
            if secret_value:
                text = text.replace(secret_value, "<redacted>")
        path.write_text(text, encoding="utf-8")


def main() -> None:
    args = _parse_args()
    if not args.confirm_live:
        raise RuntimeError(
            "Pass --confirm-live to acknowledge that this creates a paid sandbox."
        )
    if not 60 <= args.hold_seconds <= 300:
        raise RuntimeError("--hold-seconds must be between 60 and 300.")

    repo_root = Path(__file__).resolve().parents[1]
    _load_local_env(repo_root / ".env.local")
    api_key = _required_env("TENKI_API_KEY")
    api_endpoint = os.getenv("TENKI_API_ENDPOINT", "").strip() or None
    project_id = os.getenv("TENKI_PROJECT_ID", "").strip() or None
    workspace_id = os.getenv("TENKI_WORKSPACE_ID", "").strip() or None

    cloudflared = args.cloudflared.resolve()
    if not cloudflared.exists():
        raise RuntimeError(
            f"cloudflared was not found at {cloudflared}. "
            "Download the official binary before running this test."
        )

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    artifact_dir = repo_root / "build" / "e2e" / timestamp
    artifact_dir.mkdir(parents=True)
    prefect_home = artifact_dir / "prefect-home"
    prefect_home.mkdir()

    local_api_url = f"http://127.0.0.1:{args.port}/api"
    auth_string = f"tenki-e2e:{secrets.token_urlsafe(32)}"
    process_env = os.environ.copy()
    process_env.update(
        {
            "PREFECT_HOME": str(prefect_home),
            "PREFECT_API_URL": local_api_url,
            "PREFECT_API_AUTH_STRING": auth_string,
            "PREFECT_SERVER_API_AUTH_STRING": auth_string,
            "PREFECT_SERVER_ANALYTICS_ENABLED": "false",
        }
    )
    os.environ.update(process_env)

    pool_name = "tenki-e2e"
    deployment_name = "uci-iris-three-minute"
    report: dict[str, Any] = {
        "started_at": datetime.now(UTC).isoformat(),
        "hold_seconds": args.hold_seconds,
        "image": PREFECT_IMAGE,
        "source_branch": args.source_branch,
        "artifact_dir": str(artifact_dir),
    }

    server_log = artifact_dir / "prefect-server.log"
    tunnel_log = artifact_dir / "cloudflared.log"
    worker_log = artifact_dir / "prefect-worker.log"
    log_paths = [server_log, tunnel_log, worker_log]
    server: subprocess.Popen[Any] | None = None
    tunnel: subprocess.Popen[Any] | None = None
    worker: subprocess.Popen[Any] | None = None
    server_handle = tunnel_handle = worker_handle = None
    run_succeeded = False

    try:
        prefect_cli = _prefect_cli()
        server, server_handle = _start_process(
            [
                str(prefect_cli),
                "server",
                "start",
                "--host",
                "127.0.0.1",
                "--port",
                str(args.port),
            ],
            env=process_env,
            log_path=server_log,
        )
        _wait_for_http(f"{local_api_url}/health", auth_string, timeout=120)

        tunnel, tunnel_handle = _start_process(
            [
                str(cloudflared),
                "tunnel",
                "--no-autoupdate",
                "--protocol",
                "http2",
                "--url",
                f"http://127.0.0.1:{args.port}",
            ],
            env=process_env,
            log_path=tunnel_log,
        )
        tunnel_root = _wait_for_tunnel_url(tunnel, tunnel_log)
        public_api_url = f"{tunnel_root}/api"
        _wait_for_http(f"{public_api_url}/health", auth_string, timeout=90)
        report["tunnel_established"] = True

        print(
            f"Prefect UI local: http://127.0.0.1:{args.port} "
            "(endpoint público temporário protegido por autenticação).",
            flush=True,
        )

        block_id = asyncio.run(
            _create_credentials_and_pool(
                api_key=api_key,
                api_endpoint=api_endpoint,
                pool_name=pool_name,
            )
        )
        deployment_id = _create_deployment(
            block_id=block_id,
            pool_name=pool_name,
            deployment_name=deployment_name,
            public_api_url=public_api_url,
            auth_string=auth_string,
            source_branch=args.source_branch,
            hold_seconds=args.hold_seconds,
            project_id=project_id,
            workspace_id=workspace_id,
        )
        report["deployment_id"] = str(deployment_id)

        worker, worker_handle = _start_process(
            [
                str(prefect_cli),
                "worker",
                "start",
                "--pool",
                pool_name,
                "--type",
                "tenki",
                "--name",
                "tenki-e2e-worker",
                "--limit",
                "1",
            ],
            env=process_env,
            log_path=worker_log,
        )

        flow_run_id = asyncio.run(_create_flow_run(deployment_id, args.hold_seconds))
        report["flow_run_id"] = str(flow_run_id)
        report["prefect_ui_path"] = f"/runs/flow-run/{flow_run_id}"
        print(f"Flow run submetido: {flow_run_id}", flush=True)

        state, _ = asyncio.run(
            _monitor_and_download(
                flow_run_id=flow_run_id,
                api_key=api_key,
                api_endpoint=api_endpoint,
                artifact_dir=artifact_dir,
                timeout=args.hold_seconds + 420,
                report=report,
            )
        )
        report["prefect_state"] = state
        run_succeeded = state == "Completed"
        if not run_succeeded:
            raise RuntimeError(f"Prefect flow finished in state {state}.")
    except BaseException as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        _stop_process(worker, worker_handle)
        if not run_succeeded and report.get("sandbox_id"):
            try:
                asyncio.run(
                    _emergency_close(
                        report["sandbox_id"],
                        api_key,
                        api_endpoint,
                    )
                )
                report["emergency_cleanup"] = "attempted"
            except BaseException as cleanup_error:
                report["emergency_cleanup"] = (
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
        _stop_process(tunnel, tunnel_handle)
        _stop_process(server, server_handle)
        report["finished_at"] = datetime.now(UTC).isoformat()
        _sanitize_logs(log_paths, [api_key, auth_string])
        prefect_home_resolved = prefect_home.resolve()
        artifact_resolved = artifact_dir.resolve()
        if artifact_resolved in prefect_home_resolved.parents:
            shutil.rmtree(prefect_home_resolved, ignore_errors=True)
        (artifact_dir / "E2E_REPORT.json").write_text(
            json.dumps(report, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    print(
        f"E2E concluído: {report['prefect_state']}. "
        f"Relatório: {artifact_dir / 'E2E_REPORT.json'}",
        flush=True,
    )


if __name__ == "__main__":
    main()
