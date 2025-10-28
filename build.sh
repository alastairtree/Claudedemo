

uv sync --all-extras

uv run pytest

uv run ruff format --check .
# fix with uv run ruff format .

uv run ruff check .
# uv run ruff check --fix .

./generate-docs.sh build