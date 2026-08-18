"""Ponto de entrada explícito para recuperação da Outbox em reinícios."""

from .producer import OutboxPublisher


def recover_pending_events(publisher: OutboxPublisher) -> int:
    return publisher.publish_pending()
