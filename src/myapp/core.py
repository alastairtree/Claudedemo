"""Core business logic for myapp."""


def process_data(text: str, uppercase: bool = False, repeat: int = 1) -> str:
    """Process input text according to specified parameters.

    Args:
        text: The input text to process
        uppercase: Whether to convert to uppercase
        repeat: Number of times to repeat the text

    Returns:
        Processed text string

    Raises:
        ValueError: If repeat is less than 1

    Examples:
        >>> process_data("hello")
        'Hello, hello!'
        >>> process_data("world", uppercase=True)
        'HELLO, WORLD!'
        >>> process_data("hi", repeat=2)
        'Hello, hi! Hello, hi!'
    """
    if repeat < 1:
        raise ValueError("Repeat count must be at least 1")

    result = f"Hello, {text}!"

    if uppercase:
        result = result.upper()

    if repeat > 1:
        result = " ".join([result] * repeat)

    return result


def validate_input(text: str, max_length: int = 100) -> bool:
    """Validate input text against constraints.

    Args:
        text: The text to validate
        max_length: Maximum allowed length

    Returns:
        True if valid, False otherwise

    Examples:
        >>> validate_input("valid")
        True
        >>> validate_input("a" * 101)
        False
    """
    if not text or not text.strip():
        return False

    if len(text) > max_length:
        return False

    return True
