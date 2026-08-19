from event_bus.inbox import InMemoryInbox


def test_evento_duplicado_nao_repete_efeito_de_negocio__spec_AC_010() -> None:
    """@spec:AC-010 Evento duplicado não repete efeito de negócio."""
    inbox = InMemoryInbox()
    effects: list[str] = []
    event_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    if inbox.claim(event_id):
        effects.append("reserve-stock")
        inbox.complete(event_id)
    if inbox.claim(event_id):
        effects.append("reserve-stock")
        inbox.complete(event_id)

    assert effects == ["reserve-stock"]
    assert inbox.was_processed(event_id)


def test_claim_liberado_pode_ser_processado_novamente_sem_finalizar__spec_AC_026() -> None:
    """@spec:AC-026 Uma falha recuperável libera somente o estado em processamento."""
    inbox = InMemoryInbox()
    event_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    assert inbox.claim(event_id)
    inbox.release(event_id)

    assert inbox.claim(event_id)
    assert not inbox.was_processed(event_id)
