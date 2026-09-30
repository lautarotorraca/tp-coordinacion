FNV1A_64_OFFSET_BASIS = 14695981039346656037
FNV1A_64_PRIME = 1099511628211
UINT64_MASK = (1 << 64) - 1


def aggregation_owner(fruit, aggregation_amount):
    """Asigna una fruta siempre a la misma partición de Aggregation."""

    if not isinstance(fruit, str) or not fruit:
        raise ValueError("Fruit must be a non-empty string")
    if aggregation_amount <= 0:
        raise ValueError("Aggregation amount must be positive")

    value = FNV1A_64_OFFSET_BASIS
    for byte in fruit.encode("utf-8"):
        value ^= byte
        value = (value * FNV1A_64_PRIME) & UINT64_MASK
    return value % aggregation_amount
