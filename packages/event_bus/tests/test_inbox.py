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
