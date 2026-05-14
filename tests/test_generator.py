"""Проверки генератора транзакций."""
import generator


def test_make_transaction_shape():
    user_ids = ["u1", "u2", "u3"]
    tx = generator.make_transaction(user_ids)
    assert len(tx) == 7
    user_id, amount, currency, category, merchant, status, is_fraud = tx

    assert user_id in user_ids
    assert isinstance(amount, float) and amount > 0
    assert currency in generator.CURRENCIES
    assert category in generator.CATEGORIES
    assert merchant in generator.MERCHANTS[category]
    assert status in generator.STATUSES
    assert isinstance(is_fraud, bool)


def test_merchant_always_matches_category():
    user_ids = ["u1"]
    for _ in range(500):
        _, _, _, category, merchant, _, _ = generator.make_transaction(user_ids)
        assert merchant in generator.MERCHANTS[category]


def test_amount_ranges_for_fraud_and_normal():
    user_ids = ["u1"]
    for _ in range(2000):
        _, amount, _, _, _, _, is_fraud = generator.make_transaction(user_ids)
        if is_fraud:
            assert 500 <= amount <= 5000
        else:
            assert 1 <= amount <= 300


def test_reference_tables_are_consistent():
    assert set(generator.MERCHANTS.keys()) == set(generator.CATEGORIES)
    for category, merchants in generator.MERCHANTS.items():
        assert len(merchants) > 0
