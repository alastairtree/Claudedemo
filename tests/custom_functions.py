"""Helper functions for testing custom function column mappings."""


def calculate_percentage(consumed: str, total: str) -> float:
    """Calculate percentage of consumed resources.

    Args:
        consumed: Amount consumed (as string from CSV)
        total: Total amount available (as string from CSV)

    Returns:
        Percentage available
    """
    consumed_val = float(consumed)
    total_val = float(total)
    if total_val == 0:
        return 0.0
    return ((total_val - consumed_val) / total_val) * 100


def concatenate_strings(first: str, second: str) -> str:
    """Concatenate two strings with a space.

    Args:
        first: First string
        second: Second string

    Returns:
        Concatenated string
    """
    return f"{first} {second}"


def calculate_total(price: str, quantity: str) -> float:
    """Calculate total price.

    Args:
        price: Unit price (as string from CSV)
        quantity: Quantity (as string from CSV)

    Returns:
        Total price
    """
    return float(price) * float(quantity)
