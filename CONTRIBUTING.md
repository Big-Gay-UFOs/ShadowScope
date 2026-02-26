# Contributing

## Development workflow (required)

Never push directly to `main`. Always:

1. `git checkout main`
2. `git pull --ff-only`
3. `git checkout -b sprint/usaspending-<topic>`
4. Make changes locally
5. Run checks:
   - `ruff check .`
   - `python -m pytest`
6. Commit + push:
   - `git commit -m "..."` (small, focused commits)
   - `git push -u origin HEAD`
7. Open + merge PR:
   - `gh pr create`
   - `gh pr merge`

## Local setup (Windows PowerShell)

From repo root:

- (Optional) create venv:
  - `python -m venv .venv`
  - `.\.venv\Scripts\Activate.ps1`

- Install deps:
  - `python -m pip install --upgrade pip`
  - `python -m pip install -r requirements.txt`
  - `python -m pip install ruff`

## Running tests locally

CI uses SQLite. To match CI locally:

- `$env:DATABASE_URL = "sqlite:///./test_local.db"`
- `python -m pytest`

## Linting locally

- `ruff check .`
