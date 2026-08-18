from event_bus.health import HealthEndpoints


def test_health_e_readiness_distinguem_processo_de_dependencias__spec_AC_012() -> None:
    """@spec:AC-012 Saúde e prontidão distinguem processo vivo de Kafka disponível."""
    state = {"kafka": False, "postgres": True}
    endpoints = HealthEndpoints({"kafka": lambda: state["kafka"], "postgres": lambda: state["postgres"]})

    assert endpoints.health() == (200, {"status": "live"})
    unavailable = endpoints.ready()
    assert unavailable.status_code == 503
    assert unavailable.dependencies == {"kafka": "unavailable", "postgres": "available"}

    state["kafka"] = True
    assert endpoints.ready().status_code == 200
