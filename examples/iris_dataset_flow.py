"""Small Prefect flow used by the opt-in Tenki end-to-end validation."""

from __future__ import annotations

import hashlib
import json
import socket
import time
import urllib.request
import zipfile
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

DATASET_URL = "https://archive.ics.uci.edu/static/public/53/iris.zip"
DATASET_DOI = "https://doi.org/10.24432/C56C76"
REMOTE_OUTPUT_DIR = Path("/tmp/prefect-tenki-e2e")
REMOTE_DATASET_PATH = REMOTE_OUTPUT_DIR / "iris.csv"
REMOTE_MANIFEST_PATH = REMOTE_OUTPUT_DIR / "manifest.json"


@task(name="download-and-validate-uci-iris", retries=2, retry_delay_seconds=3)
def download_iris_dataset() -> dict[str, Any]:
    """Download Iris inside the sandbox and write a normalized CSV."""
    request = urllib.request.Request(
        DATASET_URL,
        headers={"User-Agent": "prefect-tenki-e2e/0.1"},
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        archive = response.read()

    with zipfile.ZipFile(BytesIO(archive)) as dataset_zip:
        members = [
            name
            for name in dataset_zip.namelist()
            if PurePosixPath(name).name == "iris.data"
        ]
        if len(members) != 1:
            raise RuntimeError(f"Expected one iris.data member, found {len(members)}.")
        source_rows = [
            line
            for line in dataset_zip.read(members[0]).decode("utf-8").splitlines()
            if line.strip()
        ]

    if len(source_rows) != 150:
        raise RuntimeError(f"Expected 150 Iris rows, found {len(source_rows)}.")

    csv_text = (
        "sepal_length_cm,sepal_width_cm,petal_length_cm,"
        "petal_width_cm,species\n" + "\n".join(source_rows) + "\n"
    )
    csv_bytes = csv_text.encode("utf-8")
    REMOTE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REMOTE_DATASET_PATH.write_bytes(csv_bytes)

    manifest: dict[str, Any] = {
        "dataset": "UCI Iris",
        "source_url": DATASET_URL,
        "doi": DATASET_DOI,
        "rows": len(source_rows),
        "bytes": len(csv_bytes),
        "sha256": hashlib.sha256(csv_bytes).hexdigest(),
        "sandbox_hostname": socket.gethostname(),
        "remote_dataset_path": str(REMOTE_DATASET_PATH),
        "created_at": datetime.now(UTC).isoformat(),
    }
    REMOTE_MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"TENKI_E2E_DATASET_READY={REMOTE_DATASET_PATH}", flush=True)
    return manifest


@flow(name="tenki-iris-e2e", log_prints=True, persist_result=True)
def tenki_iris_e2e(hold_seconds: int = 180) -> dict[str, Any]:
    """Run in Tenki, expose evidence in Prefect, and remain visible briefly."""
    if not 0 <= hold_seconds <= 300:
        raise ValueError("hold_seconds must be between 0 and 300.")

    manifest = download_iris_dataset()
    create_markdown_artifact(
        key="tenki-iris-e2e",
        description="Evidence created by the prefect-tenki live E2E flow.",
        markdown=(
            "# prefect-tenki E2E\n\n"
            f"- Dataset: [UCI Iris]({DATASET_DOI})\n"
            f"- Rows: {manifest['rows']}\n"
            f"- SHA-256: `{manifest['sha256']}`\n"
            f"- Sandbox hostname: `{manifest['sandbox_hostname']}`\n"
            f"- Remote path: `{manifest['remote_dataset_path']}`\n"
            f"- UI hold: {hold_seconds} seconds\n"
        ),
    )

    deadline = time.monotonic() + hold_seconds
    while True:
        remaining = max(0, int(deadline - time.monotonic()))
        if remaining <= 0:
            break
        print(
            f"Tenki sandbox remains available for inspection: {remaining}s",
            flush=True,
        )
        time.sleep(min(30, remaining))

    print("Tenki E2E hold complete; the worker may clean up the sandbox.", flush=True)
    return manifest
