# Development notes

## Zero-credit workflow

The normal development suite never authenticates with Tenki and never creates a
sandbox.

```powershell
uv sync --frozen
$env:PREFECT_HOME = Join-Path (Resolve-Path .).Path ".prefect-home"
uv run pytest -q
uv run ruff check src tests
uv run ruff format --check src tests
uv build
```

All lifecycle tests inject a fake SDK at the worker boundary. The installed Tenki
package is used only by a local contract test that inspects classes and signatures.

## Current development baseline

- Prefect tested locally: `3.8.0`.
- Tenki SDK tested locally: `0.4.0`.
- Worker API: `BaseWorker.run()` and `kill_infrastructure()`.
- Guest command: `prefect flow-run execute`.
- SDK surface: `AsyncClient`, `AsyncSandbox`, and `CommandResult`.

The package is deliberately located next to the Prefect clone. If Prefect requests a
standalone integration, this directory can become its repository. If a monorepo
integration is approved, the package can move under `src/integrations/prefect-tenki`.

## No-credit tests

- credentials stay masked and out of the guest environment;
- job configuration and base job template validation;
- worker type registration and package entry point;
- success, non-zero exit, and signal with exit code zero;
- create and command failures;
- cleanup after exceptions and cancellation;
- preservation of primary and cleanup failures;
- close by sandbox ID and not-found mapping;
- explicit cleanup of both the sandbox and its owning SDK client;
- SDK resource validation without requiring an optional template runtime;
- split UTF-8 output chunks;
- public async SDK contract inspection.

## Deferred until credits and external feedback are available

- actual provisioning and create failure atomicity;
- real stdout/stderr streaming and backpressure;
- timeout and signal values returned by the guest;
- idempotency of repeated remote termination;
- image or snapshot containing Prefect;
- a complete Prefect deployment run against a publicly reachable API;
- final repository, ownership, minimum Prefect version, and release metadata.

## Opt-in live worker smoke test

The live smoke test uses the real SDK and may consume Tenki credits. It is never
collected by pytest or run by CI.

First, provide the Workspace API key through the current PowerShell process. The
secure prompt keeps the value out of the command history:

```powershell
$secureKey = Read-Host "TENKI_API_KEY" -AsSecureString
$env:TENKI_API_KEY = [System.Net.NetworkCredential]::new("", $secureKey).Password
```

Validate authentication and list the accessible workspace/project IDs without
creating a sandbox:

```powershell
$env:TENKI_SMOKE_IDENTITY_ONLY = "1"
uv run --isolated --frozen python tests/live_worker_smoke.py
Remove-Item Env:TENKI_SMOKE_IDENTITY_ONLY
```

If more than one project is listed, select one:

```powershell
$env:TENKI_PROJECT_ID = "<project-id>"
```

Then create one 1-vCPU/512-MiB sandbox, run a short command, keep it visible in the
dashboard for 30 seconds, and close it:

```powershell
$env:TENKI_SMOKE_HOLD_SECONDS = "30"
uv run --isolated --frozen python tests/live_worker_smoke.py
```

Remove the credential from the shell after testing:

```powershell
Remove-Item Env:TENKI_API_KEY
Remove-Item Env:TENKI_PROJECT_ID -ErrorAction SilentlyContinue
Remove-Item Env:TENKI_WORKSPACE_ID -ErrorAction SilentlyContinue
```

The worker shields cleanup from cancellation and also applies a short maximum
duration as a server-side backstop. If the local process is forcibly killed, inspect
the Tenki dashboard and terminate any remaining smoke-test sandbox explicitly.

## Opt-in Prefect deployment E2E

The full E2E test creates an ephemeral Prefect Server, a real `tenki` work pool,
and one flow run in a Tenki sandbox. The flow downloads the
[UCI Iris dataset](https://archive.ics.uci.edu/dataset/53/iris), validates its 150
rows, remains visible for three minutes, and then exits. While it is running, the
harness retrieves `iris.csv` from the sandbox through the Tenki SDK.

The `image` worker variable expects a published Tenki Registry reference, not a
Docker Hub image. This one-off harness therefore uses the Tenki base image and
creates an ephemeral `uv` environment with Prefect 3.8.0. Remote evidence is
written below `/home/tenki`, because SDK filesystem reads reject paths outside
the guest workdir (including `/tmp`).

Because the remote sandbox cannot reach `localhost`, the harness uses an official
`cloudflared` quick tunnel. The Prefect API is protected with a random temporary
Basic Auth credential, and its ephemeral database is deleted after the run.

Download the official tunnel binary into the ignored build directory:

```powershell
New-Item -ItemType Directory -Force build/tools | Out-Null
Invoke-WebRequest `
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe `
  -OutFile build/tools/cloudflared.exe
```

The example flow must exist on the public source branch configured by the harness.
Then run:

```powershell
.\.venv\Scripts\python.exe tests/live_prefect_e2e.py `
  --confirm-live `
  --hold-seconds 180
```

During the hold, open the local Prefect UI printed by the harness and the Tenki UI.
The sandbox ID is printed as soon as the worker reports its infrastructure PID.
Afterward, inspect `build/e2e/<timestamp>/E2E_REPORT.json`, `iris.csv`, the
remote manifest, and the sanitized Prefect/worker/tunnel logs. The script stops all
child processes and confirms the remote sandbox cleanup when possible.

## Branch and review workflow

- `main` contains reviewed changes.
- Feature work uses short-lived `feat/*`, `fix/*`, `test/*`, `docs/*`, or `chore/*`
  branches.
- Changes are submitted to `main` through a pull request and squash-merged after
  the zero-credit CI checks pass.
- Live Tenki checks remain manual and opt-in; credentials must never be committed
  or exposed to pull requests from forks.
