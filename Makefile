#
#  Makefile
#

default:
	@echo 'Available commands:'
	@echo
	@echo '  make audit     Audit the schema of all available files'
	@echo '  make fetch     Fetch all data files into the data/ folder'
	@echo '  make test      Run all linting and unit tests'
	@echo '  make watch     Run all tests, watching for changes'
	@echo '  make clean     Delete any fetched data files'
	@echo

audit:
	@echo '==> Auditing JSON records'
	@poetry install &>/dev/null
	poetry run python owid/walden/audit.py

fetch:
	@echo '==> Fetching the full dataset'
	@poetry install &>/dev/null
	@poetry run python owid/walden/fetch.py

clean:
	@echo '==> Deleting all downloaded data'
	rm -rf ~/.owid/walden

test: check-formatting lint check-typing unittest

lint:
	@echo '==> Linting'
	@poetry run flake8

check-formatting:
	@echo '==> Checking formatting'
	@poetry run black --check owid/walden
	@poetry run black --check ingests/
	@poetry run python -m owid.walden.format_json --check

check-typing:
	@echo '==> Checking types'
	@poetry run mypy owid/walden

unittest:
	@echo '==> Running unit tests'
	@PYTHONPATH=. poetry run pytest

format:
	@echo '==> Reformatting files'
	@poetry run black -q owid/walden/
	@poetry run black -q ingests/
	@poetry run python owid/walden/dev/format_json.py

watch:
	poetry run watchmedo shell-command -c 'clear; make test' --recursive --drop .
