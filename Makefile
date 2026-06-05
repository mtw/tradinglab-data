# Local CI simulation for tradinglab-data.
# Mirrors .github/workflows/ci.yml so failures surface before push.
#
# Usage:
#   make ci           — full CI (lint + typecheck + test + smoke + build)
#   make ci-matrix    — test across locally available Python versions
#   make lint         — ruff only
#   make typecheck    — mypy only
#   make test         — pytest only
#   make smoke        — CLI smoke check
#   make build        — wheel + sdist + twine check
#   make clean        — remove build/dist artefacts

VENV_PY  := .venv/bin/python

.PHONY: ci ci-matrix lint typecheck test smoke build clean

ci: lint typecheck test smoke build
	@echo ""
	@echo "✓ all CI checks passed"

lint:
	$(VENV_PY) -m ruff check src tests

typecheck:
	$(VENV_PY) -m mypy src

test:
	PYTHONPATH=src $(VENV_PY) -m pytest -q tests --cov-fail-under=85

smoke:
	PYTHONPATH=src $(VENV_PY) -m tradinglab_data.cli schema --format markdown

build:
	$(VENV_PY) -m build
	$(VENV_PY) -m twine check dist/*

# CI matrix: 3.10 3.11 3.12 3.13
MATRIX_VERSIONS := 3.10 3.11 3.12 3.13

ci-matrix:
	@echo "Running pytest across available Python versions..."
	@passed=0; failed=0; skipped=0; \
	for v in $(MATRIX_VERSIONS); do \
	  py=$$(command -v python$$v 2>/dev/null); \
	  if [ -z "$$py" ]; then \
	    echo "  python$$v — SKIP (not installed)"; \
	    skipped=$$((skipped+1)); \
	    continue; \
	  fi; \
	  echo "  python$$v — $$py"; \
	  tmp=$$(mktemp -d); \
	  log=$$tmp/pytest.log; \
	  $$py -m venv $$tmp/venv 2>/dev/null; \
	  $$tmp/venv/bin/pip install -q ".[test,dev]" 2>/dev/null; \
	  if $$tmp/venv/bin/python -m pytest -q --no-header --no-cov tests >$$log 2>&1; then \
	    tail -3 $$log; \
	    passed=$$((passed+1)); \
	    echo "  python$$v — PASSED"; \
	  else \
	    tail -20 $$log; \
	    echo "  python$$v — FAILED ***"; \
	    failed=$$((failed+1)); \
	  fi; \
	  rm -rf $$tmp; \
	done; \
	echo ""; \
	echo "Matrix results: $$passed passed, $$failed failed, $$skipped skipped"; \
	[ $$failed -eq 0 ]

clean:
	rm -rf dist/ build/ src/*.egg-info
