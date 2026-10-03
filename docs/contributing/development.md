# Development Guide

## Prerequisites

!!! warning "Non-macOS Platforms"
    Building on Linux and Windows requires compiling `rusty_v8` from source, which can take 30+ minutes and requires additional dependencies (Python, Clang/LLVM, etc.). See the [rusty_v8 build documentation](https://github.com/denoland/rusty_v8?tab=readme-ov-file#build-v8-from-source) for platform-specific requirements.

- **Python**: 3.10 or higher
- **Rust**: Install via [rustup](https://rust-lang.org/tools/install/); the version is pinned in `rust-toolchain.toml` and rustup selects it automatically, keeping local builds, lints, and CI on the same toolchain
- **uv**: Fast Python package manager (install via [uv docs](https://docs.astral.sh/uv/getting-started/installation/))
- **Make**: Build automation tool (usually pre-installed on macOS/Linux)

## Quick Start

1. **Clone**: `git clone https://github.com/imfing/jsrun.git && cd jsrun`
2. **Install**: `make install` - Installs all Python dependencies using `uv`
3. **Build**: `make build-dev` - Compiles Rust code using [maturin](https://www.maturin.rs/)
4. **Test**: `make test` (or `make test-quiet` for less output)

## Common Development Tasks

- **Format code**: `make format` - Auto-format both Python and Rust code
- **Lint code**: `make lint` - Check code style without making changes
- **Fix linting**: `make lint-python-fix` - Auto-fix Python linting issues
- **Build docs**: `make docs` - Build the documentation site
- **Serve docs**: `make docs-serve` - Serve docs locally at http://127.0.0.1:8000 with live reload
- **Run CI locally**: `make all` - Run the full CI pipeline (format, build, lint, test)
- **Clean artifacts**: `make clean` - Remove build artifacts and caches

## Development Workflow

1. **Create a feature branch**: `git checkout -b feature/my-feature`
2. **Make changes**: Edit Python code in `python/jsrun/` or Rust code in `src/`
3. **Rebuild**: Run `make build-dev` after Rust changes
4. **Test**: Run `make test` to verify your changes
5. **Format and lint**: Run `make format` and `make lint`
6. **Commit**: Make commits with clear messages
7. **Run full CI**: Run `make all` before pushing
8. **Push and create PR**: Push your branch and open a pull request

## Project Structure

```
python/jsrun/        # Python API and bindings
src/                 # Rust core implementation
    lib.rs          # PyO3 module definition
    runtime/        # V8 runtime implementation
tests/              # Python test suite
docs/               # MkDocs documentation
examples/           # Usage examples
Makefile            # Development automation
pyproject.toml      # Python project configuration
```

## Tips

- **Use `make help`** to see all available Make targets
- **Development builds are faster** but production builds (`make build-prod`) are optimized for performance
- **Pre-commit hooks** run automatically after `make install` to catch issues early
- **Run tests frequently** to catch regressions quickly
- **Check the Makefile** for additional tasks and customizations

## See Also

- See [Architecture](architecture.md) for implementation details
- Check existing [issues](https://github.com/imfing/jsrun/issues) or open a new one
