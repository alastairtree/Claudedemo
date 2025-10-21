"""Tests for core business logic."""

import pytest

from myapp.core import process_data, validate_input


class TestProcessData:
    """Test suite for process_data function."""

    def test_basic_processing(self) -> None:
        """Test basic text processing without options."""
        result = process_data("world")
        assert result == "Hello, world!"

    def test_uppercase_conversion(self) -> None:
        """Test uppercase conversion option."""
        result = process_data("world", uppercase=True)
        assert result == "HELLO, WORLD!"

    def test_repeat_once(self) -> None:
        """Test repeat with count of 1 (default)."""
        result = process_data("test", repeat=1)
        assert result == "Hello, test!"

    def test_repeat_multiple(self) -> None:
        """Test repeating text multiple times."""
        result = process_data("hi", repeat=3)
        assert result == "Hello, hi! Hello, hi! Hello, hi!"

    def test_uppercase_and_repeat(self) -> None:
        """Test combining uppercase and repeat options."""
        result = process_data("test", uppercase=True, repeat=2)
        assert result == "HELLO, TEST! HELLO, TEST!"

    def test_invalid_repeat_count(self) -> None:
        """Test that invalid repeat count raises ValueError."""
        with pytest.raises(ValueError, match="Repeat count must be at least 1"):
            process_data("test", repeat=0)

        with pytest.raises(ValueError, match="Repeat count must be at least 1"):
            process_data("test", repeat=-1)

    def test_empty_string(self) -> None:
        """Test processing empty string."""
        result = process_data("")
        assert result == "Hello, !"

    def test_special_characters(self) -> None:
        """Test processing text with special characters."""
        result = process_data("hello@world!")
        assert result == "Hello, hello@world!!"

    def test_unicode_characters(self) -> None:
        """Test processing text with unicode characters."""
        result = process_data("世界")
        assert result == "Hello, 世界!"


class TestValidateInput:
    """Test suite for validate_input function."""

    def test_valid_input(self) -> None:
        """Test validation of valid input."""
        assert validate_input("valid text") is True

    def test_empty_string(self) -> None:
        """Test validation rejects empty string."""
        assert validate_input("") is False

    def test_whitespace_only(self) -> None:
        """Test validation rejects whitespace-only string."""
        assert validate_input("   ") is False
        assert validate_input("\t\n") is False

    def test_max_length_boundary(self) -> None:
        """Test validation at max length boundary."""
        # Exactly at max length should pass
        assert validate_input("a" * 100, max_length=100) is True

        # One over max length should fail
        assert validate_input("a" * 101, max_length=100) is False

    def test_custom_max_length(self) -> None:
        """Test validation with custom max length."""
        assert validate_input("short", max_length=10) is True
        assert validate_input("this is too long", max_length=10) is False

    def test_valid_unicode(self) -> None:
        """Test validation accepts unicode characters."""
        assert validate_input("Hello 世界") is True
