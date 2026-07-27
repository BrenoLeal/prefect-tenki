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
- SDK resource validation and explicit runtime readiness;
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

## Branch and review workflow

- `main` contains reviewed changes.
- Feature work uses short-lived `feat/*`, `fix/*`, `test/*`, `docs/*`, or `chore/*`
  branches.
- Changes are submitted to `main` through a pull request and squash-merged after
  the zero-credit CI checks pass.
- Live Tenki checks remain manual and opt-in; credentials must never be committed
  or exposed to pull requests from forks.
