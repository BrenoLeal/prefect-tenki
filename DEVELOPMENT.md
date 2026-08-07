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
- Tenki SDK tested locally: `0.5.4`.
- Worker API: `BaseWorker.run()` and `kill_infrastructure()`.
- Guest command: `prefect flow-run execute`.
- SDK namespace and surface: `tenki.AsyncClient`, `tenki.AsyncSandbox`, and `tenki.CommandResult`.

`tenki-prefect` is maintained as a standalone repository. If a future Prefect
monorepo contribution is approved, the package can move to the integration path
requested by Prefect maintainers.

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
- terminal-state reconciliation when the SDK cache lags the Tenki server;
- SDK resource validation without requiring an optional template runtime;
- split UTF-8 output chunks;
- public async SDK contract inspection.

## Manual live validation

Live checks remain opt-in and never run in CI because they authenticate with Tenki
and may consume credits. Manual validation with Tenki SDK 0.5.4 has covered:

- real sandbox provisioning and cleanup;
- stdout/stderr streaming, exit codes, timeouts, and cancellation;
- repeated remote termination and credential isolation;
- a complete Prefect deployment run against an authenticated public API;
- remote dataset retrieval and a public HTTP/WebSocket application.

A reusable image or snapshot with Prefect preinstalled, final release versioning,
and package publication remain follow-up work.

## Opt-in live worker smoke test

The live smoke test uses the real SDK and may consume Tenki credits. It is never
collected by pytest or run by CI.

First, provide the Workspace API key through the current PowerShell process. The
secure prompt keeps the value out of the command history:

```powershell
$secureKey = Read-Host "TENKI_API_KEY" -AsSecureString
$env:TENKI_API_KEY = [System.Net.NetworkCredential]::new("", $secureKey).Password
```

Validate authentication and list the accessible workspace IDs without
creating a sandbox:

```powershell
$env:TENKI_SMOKE_IDENTITY_ONLY = "1"
uv run --isolated --frozen python tests/live_worker_smoke.py
Remove-Item Env:TENKI_SMOKE_IDENTITY_ONLY
```

If more than one workspace is listed, select one:

```powershell
$env:TENKI_WORKSPACE_ID = "<workspace-id>"
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
Remove-Item Env:TENKI_WORKSPACE_ID -ErrorAction SilentlyContinue
```

The worker shields cleanup from cancellation and also applies a short maximum
duration as a server-side backstop. If the local process is forcibly killed, inspect
the Tenki dashboard and terminate any remaining smoke-test sandbox explicitly.

## Branch and review workflow

- `main` contains reviewed changes.
- Feature work uses short-lived `feat/*`, `fix/*`, `test/*`, `docs/*`, or `chore/*`
  branches.
- Changes are submitted to `main` through a pull request and squash-merged after
  the zero-credit CI checks pass.
- Live Tenki checks remain manual and opt-in; credentials must never be committed
  or exposed to pull requests from forks.
