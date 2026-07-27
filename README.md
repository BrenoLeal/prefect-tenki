# prefect-tenki

[![CI](https://github.com/BrenoLeal/prefect-tenki/actions/workflows/ci.yml/badge.svg)](https://github.com/BrenoLeal/prefect-tenki/actions/workflows/ci.yml)

`prefect-tenki` is a Prefect worker integration for running flow runs in isolated,
ephemeral [Tenki](https://tenki.cloud) sandboxes.

> [!IMPORTANT]
> This project is in early development. Its public API, packaging, and eventual
> contribution path may change based on feedback from Prefect and Tenki.

## What it does

The `tenki` worker creates one sandbox per Prefect flow run, executes Prefect's
prepared `prefect flow-run execute` command, streams guest output into worker logs,
and closes the sandbox when the run finishes, fails, or is cancelled.

The current implementation includes:

- Prefect collection and worker-type registration;
- a masked `TenkiCredentials` block;
- configurable project, workspace, compute, image, network, and timeout options;
- an asynchronous boundary around `tenki-sandbox` 0.4.x;
- cancellation-safe creation and cleanup;
- remote termination by Prefect infrastructure identifier;
- mocked lifecycle and SDK-contract tests that do not create Tenki resources.

## Requirements

- Python 3.10 or newer;
- Prefect 3.7.2 or newer, below 4.0;
- `tenki-sandbox` 0.4.x;
- for a real flow run, a Tenki image or snapshot containing Prefect and the flow's
  runtime dependencies, with outbound access to the configured Prefect API.

## Install from source

The package has not been published to PyPI. To inspect the current development
version:

```bash
git clone https://github.com/BrenoLeal/prefect-tenki.git
cd prefect-tenki
uv sync --frozen
```

Verify that the collection and worker can be imported:

```bash
uv run python -c "import prefect_tenki; print(prefect_tenki.TenkiWorker.type)"
```

The command should print `tenki`.

## Development

The default test suite is deliberately zero-credit: it does not authenticate with
Tenki or provision a sandbox.

```bash
uv sync --frozen
uv run pytest -q
uv run ruff check src tests
uv run ruff format --check src tests
uv build
```

See [DEVELOPMENT.md](DEVELOPMENT.md) for the tested SDK surface, lifecycle guarantees,
the zero-credit workflow, and the opt-in live worker smoke test.

## Contribution status

This repository is used for internal review of the standalone package while the
preferred upstream contribution path is discussed with Prefect maintainers. It is
not currently an official Prefect-maintained integration.

## License

Licensed under the [Apache License 2.0](LICENSE).
