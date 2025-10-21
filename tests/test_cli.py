"""Tests for CLI commands."""

from click.testing import CliRunner

from myapp import __version__
from myapp.cli import main


class TestCLIBasics:
    """Test suite for basic CLI functionality."""

    def test_main_group_help(self, cli_runner: CliRunner) -> None:
        """Test main command group shows help."""
        result = cli_runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "A robust CLI application" in result.output

    def test_version_option(self, cli_runner: CliRunner) -> None:
        """Test --version flag displays version."""
        result = cli_runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output


class TestGreetCommand:
    """Test suite for greet command."""

    def test_greet_basic(self, cli_runner: CliRunner) -> None:
        """Test basic greet command."""
        result = cli_runner.invoke(main, ["greet", "world"])
        assert result.exit_code == 0
        assert "Hello, world!" in result.output

    def test_greet_uppercase(self, cli_runner: CliRunner) -> None:
        """Test greet with uppercase flag."""
        result = cli_runner.invoke(main, ["greet", "world", "--uppercase"])
        assert result.exit_code == 0
        assert "HELLO, WORLD!" in result.output

    def test_greet_uppercase_short_flag(self, cli_runner: CliRunner) -> None:
        """Test greet with uppercase short flag."""
        result = cli_runner.invoke(main, ["greet", "world", "-u"])
        assert result.exit_code == 0
        assert "HELLO, WORLD!" in result.output

    def test_greet_repeat(self, cli_runner: CliRunner) -> None:
        """Test greet with repeat option."""
        result = cli_runner.invoke(main, ["greet", "test", "--repeat", "2"])
        assert result.exit_code == 0
        assert "Hello, test! Hello, test!" in result.output

    def test_greet_repeat_short_flag(self, cli_runner: CliRunner) -> None:
        """Test greet with repeat short flag."""
        result = cli_runner.invoke(main, ["greet", "test", "-r", "3"])
        assert result.exit_code == 0
        assert result.output.count("Hello, test!") == 3

    def test_greet_combined_options(self, cli_runner: CliRunner) -> None:
        """Test greet with multiple options combined."""
        result = cli_runner.invoke(main, ["greet", "cli", "-u", "-r", "2"])
        assert result.exit_code == 0
        assert "HELLO, CLI!" in result.output
        assert result.output.count("HELLO, CLI!") == 2

    def test_greet_missing_argument(self, cli_runner: CliRunner) -> None:
        """Test greet without required argument fails."""
        result = cli_runner.invoke(main, ["greet"])
        assert result.exit_code != 0
        assert "Missing argument" in result.output

    def test_greet_help(self, cli_runner: CliRunner) -> None:
        """Test greet command help."""
        result = cli_runner.invoke(main, ["greet", "--help"])
        assert result.exit_code == 0
        assert "Greet with the provided INPUT_TEXT" in result.output
        assert "--uppercase" in result.output
        assert "--repeat" in result.output


class TestInfoCommand:
    """Test suite for info command."""

    def test_info_default(self, cli_runner: CliRunner) -> None:
        """Test info command with default format."""
        result = cli_runner.invoke(main, ["info"])
        assert result.exit_code == 0
        assert "Name:" in result.output
        assert "myapp" in result.output
        assert "Version:" in result.output
        assert __version__ in result.output

    def test_info_text_format(self, cli_runner: CliRunner) -> None:
        """Test info command with explicit text format."""
        result = cli_runner.invoke(main, ["info", "--format", "text"])
        assert result.exit_code == 0
        assert "myapp" in result.output

    def test_info_json_format(self, cli_runner: CliRunner) -> None:
        """Test info command with JSON format."""
        result = cli_runner.invoke(main, ["info", "--format", "json"])
        assert result.exit_code == 0
        assert '"name": "myapp"' in result.output
        assert '"version":' in result.output

    def test_info_yaml_format(self, cli_runner: CliRunner) -> None:
        """Test info command with YAML format (not yet implemented)."""
        result = cli_runner.invoke(main, ["info", "--format", "yaml"])
        assert result.exit_code == 0
        assert "not yet implemented" in result.output.lower()

    def test_info_format_short_flag(self, cli_runner: CliRunner) -> None:
        """Test info with format short flag."""
        result = cli_runner.invoke(main, ["info", "-f", "json"])
        assert result.exit_code == 0
        assert '"name": "myapp"' in result.output

    def test_info_invalid_format(self, cli_runner: CliRunner) -> None:
        """Test info with invalid format fails."""
        result = cli_runner.invoke(main, ["info", "--format", "invalid"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output

    def test_info_help(self, cli_runner: CliRunner) -> None:
        """Test info command help."""
        result = cli_runner.invoke(main, ["info", "--help"])
        assert result.exit_code == 0
        assert "Display application information" in result.output
        assert "--format" in result.output
