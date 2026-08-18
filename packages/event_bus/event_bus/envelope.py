"""Envelope versionado e validado para todos os eventos de domínio."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from uuid import UUID, uuid4


class EnvelopeValidationError(ValueError):
    """Erro de contrato que deve ser isolado diretamente na DLQ."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(frozen=True)
class EventEnvelope:
    """Evento com identidade estável em retries e versão explícita."""

    event_id: str
    event_type: str
    event_version: int
    occurred_at: datetime
    producer: str
    correlation_id: str
    payload: Mapping[str, Any]
    causation_id: Optional[str] = None
    retry_attempt: int = 0

    def __post_init__(self) -> None:
        errors: list[str] = []
        for field_name in ("event_id", "correlation_id"):
            try:
                UUID(str(getattr(self, field_name)))
            except (ValueError, TypeError, AttributeError):
                errors.append(f"{field_name} must be a UUID")
        if self.causation_id is not None:
            try:
                UUID(str(self.causation_id))
            except (ValueError, TypeError, AttributeError):
                errors.append("causation_id must be a UUID when supplied")
        if not isinstance(self.event_type, str) or not self.event_type.strip():
            errors.append("event_type must be a non-empty string")
        if not isinstance(self.event_version, int) or isinstance(self.event_version, bool) or self.event_version < 1:
            errors.append("event_version must be a positive integer")
        if not isinstance(self.producer, str) or not self.producer.strip():
            errors.append("producer must be a non-empty string")
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None:
            errors.append("occurred_at must be a timezone-aware datetime")
        if not isinstance(self.payload, Mapping):
            errors.append("payload must be an object")
        if not isinstance(self.retry_attempt, int) or isinstance(self.retry_attempt, bool) or self.retry_attempt < 0:
            errors.append("retry_attempt must be a non-negative integer")
        if errors:
            raise EnvelopeValidationError(errors)

    @classmethod
    def new(cls, *, event_type: str, producer: str, correlation_id: str, payload: Mapping[str, Any], causation_id: Optional[str] = None, event_version: int = 1, event_id: Optional[str] = None, occurred_at: Optional[datetime] = None) -> "EventEnvelope":
        return cls(event_id=event_id or str(uuid4()), event_type=event_type, event_version=event_version, occurred_at=occurred_at or datetime.now(timezone.utc), producer=producer, correlation_id=correlation_id, causation_id=causation_id, payload=dict(payload))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EventEnvelope":
        if not isinstance(value, Mapping):
            raise EnvelopeValidationError(["envelope must be an object"])
        raw_time = value.get("occurred_at")
        if isinstance(raw_time, str):
            try:
                raw_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
            except ValueError:
                raw_time = None
        return cls(event_id=value.get("event_id"), event_type=value.get("event_type"), event_version=value.get("event_version"), occurred_at=raw_time, producer=value.get("producer"), correlation_id=value.get("correlation_id"), causation_id=value.get("causation_id"), payload=value.get("payload"), retry_attempt=value.get("retry_attempt", 0))

    def for_retry(self, attempt: int) -> "EventEnvelope":
        return replace(self, retry_attempt=attempt)

    def to_dict(self) -> dict[str, Any]:
        return {"event_id": self.event_id, "event_type": self.event_type, "event_version": self.event_version, "occurred_at": self.occurred_at.isoformat(), "producer": self.producer, "correlation_id": self.correlation_id, "causation_id": self.causation_id, "payload": dict(self.payload), "retry_attempt": self.retry_attempt}
