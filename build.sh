

uv sync --all-extras


# if arg1 is "fast" run tests without postgres
if [ "$1" = "fast" ]; then
    uv run pytest -v tests -k "not postgres"
else
    uv run pytest -v tests
fi

uv run ruff format --check .
# fix with uv run ruff format .

uv run ruff check .
# uv run ruff check --fix .

uv run mypy src

./generate-docs.sh build