.DEFAULT_GOAL := all
sources = python/jsrun tests

# using pip install cargo (via maturin via pip) doesn't get the tty handle
# so doesn't render color without some help
export CARGO_TERM_COLOR=$(shell (test -t 0 && echo "always") || echo "auto")
# maturin develop only makes sense inside a virtual env, is otherwise
# more or less equivalent to pip install -e just a little nicer
USE_MATURIN = $(shell [ "$VIRTUAL_ENV" != "" ] && (which maturin))

.PHONY: .uv  ## Check that uv is installed
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: install  ## Install the package, dependencies, and pre-commit for local development
install: .uv
	uv sync --frozen --group all

.PHONY: rebuild-lockfiles  ## Rebuild lockfiles from scratch, updating all dependencies
rebuild-lockfiles: .uv
	uv lock --upgrade

.PHONY: build-dev  ## Build the development version of the package
build-dev:
	@rm -f python/jsrun/*.so
	uv run maturin develop --uv

.PHONY: build-prod  ## Build the production version of the package
build-prod:
	@rm -f python/jsrun/*.so
	uv run maturin develop --uv --release

.PHONY: build-profiling  ## Build the profiling version of the package
build-profiling:
	@rm -f python/jsrun/*.so
	uv run maturin develop --uv --profile profiling

.PHONY: format  ## Auto-format rust and python source files
format:
	uv tool run ruff format $(sources)
	cargo fmt

.PHONY: lint-python  ## Lint python source files
lint-python:
	uv tool run ruff check $(sources)
	uv tool run ruff format --check $(sources)

.PHONY: lint-python-fix  ## Auto-fix python linting issues
lint-python-fix:
	uv tool run ruff check --fix $(sources)
	uv tool run ruff format $(sources)

.PHONY: lint-rust  ## Lint rust source files
lint-rust:
	cargo fmt --all -- --check
	cargo clippy --tests -- -D warnings

.PHONY: lint  ## Lint rust and python source files
lint: lint-python lint-rust

.PHONY: test  ## Run all tests
test:
	uv sync --frozen --group testing
	uv run python -m pytest tests/ -v

.PHONY: test-quiet  ## Run tests quietly
test-quiet:
	uv sync --frozen --group testing
	uv run python -m pytest tests/ -q

.PHONY: docs  ## Build the documentation
docs:
	uv run mkdocs build

.PHONY: docs-serve  ## Build and serve the documentation
docs-serve:
	uv run mkdocs serve --livereload

.PHONY: all  ## Run the standard set of checks performed in CI
all: format build-dev lint test

.PHONY: clean  ## Clear local caches and build artifacts
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf *.egg-info
	rm -rf build
	rm -rf site
	rm -f python/jsrun/*.so

.PHONY: help  ## Display this message
help:
	@grep -E \
		'^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-19s\033[0m %s\n", $$2, $$3}'
