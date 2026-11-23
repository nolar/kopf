# AGENTS.md

This file provides guidance to coding agents when working with the Kopf codebase.

## Project Overview

Kopf (Kubernetes Operator Pythonic Framework) is a Python framework for building Kubernetes operators. It lets developers write event-driven handler functions (decorated with `@kopf.on.event()`, `@kopf.daemon()`, etc.) that react to Kubernetes resource changes. The framework handles watch streams, cause detection, handler retries, progress persistence, finalizers, peering, admission webhooks, daemons, and timers.

- **Language:** Python 3.11+
- **License:** MIT
- **Docs:** https://docs.kopf.dev/
- **Repo:** https://github.com/nolar/kopf

## Common Commands

```bash
# Run all unit tests (E2E skipped by default)
pytest -v

# Run specific test file or directory
pytest -v tests/k8s/test_patching.py
pytest -v tests/handling/

# Run tests matching keyword
pytest -v -k "finalizer"

# Run with E2E tests (requires a k8s cluster)
pytest --with-e2e

# Run only E2E tests
pytest --only-e2e

# Type checking (strict mode)
mypy

# Pre-commit hooks (linting, formatting, import layers)
pre-commit run --all-files

# Build docs locally
sphinx-build docs/ docs/_build/ -b html --write-all
```

## Architecture

The codebase enforces strict layered architecture via `import-linter` (config in `.importlinter`). Higher layers may import lower layers, never the reverse.

### Layer Hierarchy

Modules and packages are layered, so that only higher-level modules could import lower-level modules, but not vice versa. See `docs/architecture.rst` for the detailed explanation of each module/layer and architecture diagram.

### Key Concepts

- **Watch-events:** The raw dictionaries coming from the watch-streams of K8s API, usually with fields ``type`` and ``object``
- **Causes:** Watch events are translated to high-level causes (CREATE, UPDATE, DELETE, RESUME, NOOP, FREE, GONE) in `kopf/_core/intents/causes.py`
- **Handlers:** Registered via decorators, stored in `OperatorRegistry`, selected and executed per-cause
- **Progress:** Handler execution state is persisted to the K8s object (annotations or status) so it survives operator restarts
- **Patches:** Accumulated changes applied via `kopf/_cogs/clients/patching.py` as merge-patch (body, status) and JSON Patch (finalizers)
- **Finalizers:** Block object deletion until delete-handlers complete; managed atomically via JSON Patch with optimistic concurrency
- **Peering:** Multi-operator coordination to avoid double-processing
- **Daemons/Timers:** Long-running per-object background tasks managed by `kopf/_core/engines/daemons.py`

## Testing

### Framework

- **pytest** with **pytest-asyncio** (auto mode, function-scoped event loops)
- **looptime** for deterministic async time simulation (enabled by default via `--looptime` in `addopts`)
- **kmock** for mocking HTTP responses in K8s API client tests
- **pytest-mock** for general mocking
- All warnings are treated as errors (`filterwarnings = error` in conftest)

### Principles

- Avoid mocks where possible: they test the specific implementation and must be changed when the implementation changes, even if the promised behaviour stays the same. Prefer a local mock server (e.g. KMock, which mocks any HTTP, generic, or Kubernetes API) over mocking the API client.
- Flat test functions with fixtures; no test classes.
- In tests, import the unit under test directly (e.g. `from kopf._cogs.structs.bodies import Body`) — unlike in production code (see Imports below).

### Test Organization

Tests mirror the package structure:

| Directory | Tests for |
|-----------|-----------|
| `tests/k8s/` | K8s API client wrappers (`kopf/_cogs/clients/`) |
| `tests/handling/` | Handler cause detection, execution, multi-step, daemons |
| `tests/reactor/` | Processing, queueing, observation, orchestration |
| `tests/registries/` | Handler registration and matching |
| `tests/posting/` | Kubernetes event posting |
| `tests/peering/` | Multi-operator peering |
| `tests/settings/` | Configuration and settings |
| `tests/authentication/` | Vault, credentials, login |
| `tests/e2e/` | End-to-end tests (require real cluster, skipped by default) |
| `tests/test_*.py` | Root-level tests for finalizers, diffs, bodies, etc. |

### Key Fixtures (from `tests/conftest.py`)

- **`resource`** — parametrized `Resource` (namespaced + cluster variants)
- **`namespace`** — `'ns'` for namespaced, `None` for cluster
- **`registry`** — fresh `OperatorRegistry`, auto-used (resets global registry per test)
- **`settings`** — fresh `OperatorSettings`
- **`memories`** — fresh `ResourceMemories`
- **`k8s_mocked`** — mocks `kopf._cogs.clients.api.{get,post,patch,delete,stream}`, returns `{}` by default
- **`fake_vault`** — populated auth vault for API calls
- **`assert_logs`** — assert log messages match patterns in order
- **`kmock`** — the default K8s API mock server running locally and configured as the target for the framework
- **`looptime`** — the fixture to assert on the event loop's deterministic time with no jitter

### Handling Test Fixtures (from `tests/handling/conftest.py`)

Tests in `tests/handling/` use a `cause_mock` fixture that patches cause detection, allowing tests to set `cause_mock.reason = Reason.CREATE` directly. The `handlers` fixture registers mock handlers and provides `.create_mock`, `.update_mock`, `.delete_mock`, `.resume_mock` for assertions.

### Daemon Test Fixtures (from `tests/handling/daemons/conftest.py`)

- **`simulate_cycle`** — simulates a full processing cycle; applies both merge-patch and JSON Patch responses back to the event object (mimicking K8s behavior)
- **`dummy`** — `DaemonDummy` with `.mock` and `.wait_for_daemon_done()`
- **`background_daemon_killer`** — runs the daemon killer task in background

### Assertions

When you assert merge-patch or json-patch payloads, compare the whole value of that payload with the expected one (a list or a dict), but every patch operation and its payload separately. To get all payloads as a list, use something like this (if the `k8s_mocked` fixture is used; or an equivalent if not):

```python
payloads = [call.kwargs['payload'] for call in k8s_mocked.patch.call_args_list]
assert len(payloads) == 2
assert payloads[0] == {'status': {'merge-patch': 'hello'}}
assert payloads[1] == [
    {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '123'},
    {'op': 'add', 'path': '/status', 'value': {'json-patch': 'hello'}},
]
```

## Code Style

### Typing

- **mypy strict mode** is enforced (config in `pyproject.toml`)
- Use Python 3.10+ type syntax (`X | Y` unions, `list[T]` generics)
- `TypedDict` for structured dicts, dataclasses for immutable records

### Imports

- In production code, import modules, not individual units (classes, functions, constants), so that mocking, if any, is performed at the source rather than at the use site: `from kopf._cogs.structs import bodies`, then use `bodies.Body` — not `from kopf._cogs.structs.bodies import Body`. The public re-exports (`kopf/__init__.py`, `kopf/testing.py`) are the exception.
- isort is configured in `pyproject.toml` and enforces the sorting and layout of the imports
- Internal modules use underscore prefix (`_cogs`, `_core`, `_kits`), so that no one imports them
- Third-party K8s clients (e.g., `pykube`, `kubernetes`) are only imported conditionally in two specific modules

### Formatting

- No configured auto-formatter (no black/ruff format)
- Line length ~100 (isort) but not strictly enforced for code
- Pre-commit hooks enforce: trailing whitespace, end-of-file newline, check-ast, pyupgrade

### Commits

- The title starts with a verb and continues the sentence "When merged, this commit will …" (e.g. "Improve the performance of the watch-streams"), approximately 50–70 characters.
- Focus on the purpose of the change (why, what for), not on the implementation (what, how). Avoid the verbs "add", "change", "modify", "delete", "remove" in relation to units, classes, files, or modules — that is visible in the diff.
- Do not list file names, or summarize the tests in the message — that is visible in the diff. Explain the goals and the overall solution instead.
- The message body is written as normal paragraphs, without line wrapping.
- The human user must add their "Signed-off-by" header to commits (maybe automatically).

See [contributing.rst](docs/contributing.rst) for more guidelines and conventions.

### Documentation

- RST format in `docs/`
- Markdown for the top-level text files, such as README.
- Full words preferred ("it is" not "it's"; "we have not" not "we haven't")
- Use `:kwarg:\`...\`` for handler keyword arguments
- Explicit `code-block: <language>` directives over implicit `::` ones.
- `**_` for unused kwargs instead of `**kwargs` in examples and docs.
- 1st-level page titles: double `=` overline + underline. 2nd-level sections: single `=`. 3rd-level subsections: `-` underline. 4th-level sub-subsections: `~` underline.

## CI/CD

Workflows in `.github/workflows/`:

- **ci.yaml** — runs on PRs: linting (mypy, pre-commit, import-linter) + unit tests across supported Python version with matrix of extras
- **thorough.yaml** — runs on main pushes, and weekly: full CI + integration tests + E2E with K3s
- **publish.yaml** — package publication
- **docker.yaml** — build dev-mode docker images

## Rollout/rollback plans

When designing a feature, always consider both the gradual rollout and the rollback plans. The changes must be backwards compatible. We do not control which versions of Kopf users use, how they roll out the upgrades, and how and when they roll back.
