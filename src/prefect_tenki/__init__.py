"""Prefect integration for Tenki sandboxes."""

from importlib.metadata import PackageNotFoundError, version

from prefect_tenki.credentials import TenkiCredentials
from prefect_tenki.worker import (
    TenkiWorker,
    TenkiWorkerJobConfiguration,
    TenkiWorkerResult,
    TenkiWorkerVariables,
)

try:
    __version__ = version("prefect-tenki")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "TenkiCredentials",
    "TenkiWorker",
    "TenkiWorkerJobConfiguration",
    "TenkiWorkerResult",
    "TenkiWorkerVariables",
    "__version__",
]
