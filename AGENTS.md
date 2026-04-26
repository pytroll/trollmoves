# Instructions for AI tools on pytroll/trollmoves

Purpose
- Short guide for AI sessions to help with builds, tests, architecture, and repository-specific conventions.

Build, test, and lint commands
- Ask the user which Python to use
- Install (editable): pip install -e .
- Install all extras: pip install -e .[all]
- Build sdist/wheel: python setup.py sdist bdist_wheel
- Run full test suite: pytest
- Run a single test: pytest path/to/test_file.py::test_function (e.g. pytest trollmoves/tests/test_example.py::test_xyz)
- Run tests matching name: pytest -k <expr>
- Lint: flake8 . (configured via setup.cfg; max-line-length=120)

High-level architecture
- Core package: `trollmoves/` contains modules for Server, Client, Mirror, Dispatcher, Fetcher and Movers:
  - server.py: watches directories and publishes announcements (Posttroll).
  - client.py: subscribes and requests transfers from Server.
  - mirror.py: bridge between internal/external networks.
  - dispatcher.py: pushes local files to configured destinations.
  - fetcher.py / s3downloader.py: fetch files from sources (S3, etc.).
  - movers.py: implementations for transfer protocols (FileMover, ScpMover, SftpMover, S3Mover, FtpMover).
- Messaging: Posttroll is used to announce and request transfers; systems integrate by publishing/subscribing to Posttroll topics.
- Entry points & scripts: console script `pytroll-fetcher` and bin scripts (move_it_*.py, dispatcher.py, s3downloader.py) provide CLI access.
- Versioning: versioneer is used; version file is `trollmoves/version.py`, tag prefix `v`.

Key conventions and repo specifics
- Extras groups: defined in setup.py under extras_require (e.g., 's3', 'server', 'remote_fs', 'fetcher', 'all'). Use these to install optional movers/protocol dependencies.
- S3Mover path semantics: trailing slash on destination means “keep original filename”; no trailing slash means the destination's last path segment is the new filename.
- Linting: setup.cfg contains flake8 rules (max-line-length 120) and ignores (RST303, W504).
- Tests: pytest is required; tests live under `trollmoves/tests` (or inside package as a tests package). Tests_require lists pytest-reraise and pytest-bdd when running behavior tests.
- Coverage/packaging: versioneer and version.py are excluded from coverage; packaging scripts and console entry points are defined in setup.py.

Files of interest for AI tools
- README.md: project overview and mover details (used for architecture cues).
- setup.py / setup.cfg: install, extras, flake8, versioneer config.
- trollmoves/movers.py: look here for protocol-specific logic and S3 behavior.
- bin/* and entry_points: show CLI surface and expected runtime scripts.

When using AI tools in this repo
- Prefer locating behavior across multiple files: transfers are composed from Posttroll messages (server/client) + mover implementations.
- If changing protocol behavior, update movers.py and add integration tests exercising the server/client flow.
- Respect extras in setup.py when suggesting dependency additions: put optional deps in the correct extras group.


