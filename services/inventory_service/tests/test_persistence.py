from services.inventory_service.inventory_service.persistence import InventoryRepository


def test_persistencia_mantem_reserva_e_inbox_processada():
    repository = InventoryRepository({"tea": 2})

    repository.reserve("order-1", [{"sku": "tea", "quantity": 1}])
    repository.mark_processed("evt-1")

    assert repository.available("tea") == 1
    assert repository.was_processed("evt-1")
