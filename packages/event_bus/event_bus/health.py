"""Saúde de processo e prontidão de dependências separadas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping

DependencyProbe = Callable[[], bool]


@dataclass(frozen=True)
class ReadinessReport:
    status: str
    dependencies: Mapping[str, str]

    @property
    def status_code(self) -> int:
        return 200 if self.status == "ready" else 503


class HealthEndpoints:
    """Contrato que adaptadores FastAPI expõem em ``/health`` e ``/ready``."""

    def __init__(self, dependencies: Mapping[str, DependencyProbe]) -> None:
        self._dependencies = dict(dependencies)

    def health(self) -> tuple[int, dict[str, str]]:
        return 200, {"status": "live"}

    def ready(self) -> ReadinessReport:
        statuses: dict[str, str] = {}
        for name, probe in self._dependencies.items():
            try:
                available = bool(probe())
            except Exception:
                available = False
            statuses[name] = "available" if available else "unavailable"
        return ReadinessReport("ready" if all(value == "available" for value in statuses.values()) else "not_ready", statuses)
