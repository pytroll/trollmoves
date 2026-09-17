# Instructions for AI tools on pytroll/trollmoves

Purpose
- Short guide for AI sessions to help with builds, tests, architecture, and repository-specific conventions.

Build, test, and lint commands
- Ask the user which Python to use
- Install (editable): pip install -e .
- Install all extras: pip install -e .[all]
- Build sdist/wheel: python -m build (hatchling backend)
- Run full test suite: pytest
- Run a single test: pytest path/to/test_file.py::test_function (e.g. pytest trollmoves/tests/test_example.py::test_xyz)
- Run tests matching name: pytest -k <expr>
- Lint: ruff check . (configured in pyproject.toml; line-length 120)
- Hooks: pre-commit run --all-files (ruff, trailing whitespace, end-of-file fixer)

High-level architecture
- Core package: `trollmoves/` contains modules for Server, Client, Mirror, Dispatcher, Fetcher and Movers:
  - server.py: watches directories and publishes announcements (Posttroll).
  - client.py: subscribes and requests transfers from Server.
  - mirror.py: bridge between internal/external networks.
  - dispatcher.py: pushes local files to configured destinations.
  - fetcher.py / s3downloader.py: fetch files from sources (S3, etc.).
  - movers.py: implementations for transfer protocols (FileMover, ScpMover, SftpMover, S3Mover, FtpMover).
- Messaging: Posttroll is used to announce and request transfers; systems integrate by publishing/subscribing to Posttroll topics.
- Entry points & scripts: `[project.scripts]` in pyproject.toml exposes `pytroll-fetcher` plus the move_it_*.py, dispatcher.py, remove_it.py and s3downloader.py commands. There is no `bin/` directory.
- Versioning: hatch-vcs derives the version from git tags. `trollmoves/version.py` is a generated leftover and is excluded from linting.

Key conventions and repo specifics
- Optional dependencies: defined in pyproject.toml under `[project.optional-dependencies]` ('s3', 'server', 'remote_fs', 'fetcher', 'docs', 'all'). Use these to install optional movers/protocol dependencies.
- S3Mover path semantics: trailing slash on destination means “keep original filename”; no trailing slash means the destination's last path segment is the new filename.
- Linting: pyproject.toml holds the ruff config (line-length 120; rules A, E, W, F, I, TID, Q, T10; google docstring convention; max-complexity 10). `trollmoves/version.py` is excluded.
- Tests: pytest is required; tests live under `trollmoves/tests`. Tests needing a live localhost SSH/FTP server are marked `slow`.
- Packaging is fully declarative: everything lives in pyproject.toml. There is no setup.py or setup.cfg.

Files of interest for AI tools
- README.md: project overview and mover details (used for architecture cues).
- pyproject.toml: dependencies, optional-dependency groups, ruff config, console scripts, pytest markers.
- .pre-commit-config.yaml: the hooks CI expects to pass.
- trollmoves/movers.py: look here for protocol-specific logic and S3 behavior.
- docs/source/: Sphinx documentation; add new pages to the toctree in index.rst.

Development practices
- Follow the red-green-refactor TDD cycle:
  1. Red: write a failing test that captures the intended behaviour before writing any implementation.
  2. Green: write the minimal production code needed to make the test pass — no more.
  3. Refactor: improve the code (naming, structure, duplication) while keeping all tests green.
- Tests must be committed together with — or before — the implementation they cover. Do not add production code without a corresponding test.
- Keep functions and methods small and focused on a single responsibility.
- Use descriptive names for variables, functions, and classes; avoid abbreviations and single-letter names except in short loops.
- Write comments only to explain *why*, not *what*; the code itself should make the what obvious.
- Avoid deep nesting; prefer early returns and guard clauses.
- Refactoring steps must not change observable behaviour; run the test suite before and after each refactor step to confirm.

When using AI tools in this repo
- Prefer locating behavior across multiple files: transfers are composed from Posttroll messages (server/client) + mover implementations.
- If changing protocol behavior, update movers.py and add integration tests exercising the server/client flow.
- Respect the optional-dependency groups in pyproject.toml when suggesting dependency additions: put optional deps in the correct group (and in 'all').
