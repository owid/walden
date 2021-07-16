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
	poetry run python owid/walden/dev/audit.py

fetch:
	@echo '==> Fetching the full dataset'
	@poetry install &>/dev/null
	@poetry run python owid/walden/dev/fetch.py

clean:
	@echo '==> Deleting all downloaded data'
	rm -rf data

test: check-formatting lint check-typing unittest

lint:
	@echo '==> Linting'
	@poetry run flake8

check-formatting:
	@echo '==> Checking formatting'
	@poetry run black --check -q owid/walden
	@poetry run black --check -q ingests/
	@poetry run python owid/walden/dev/format_json.py --check

check-typing:
	@echo '==> Checking types'
	@poetry run mypy owid/walden/dev

unittest:
	@echo '==> Running unit tests'
	@poetry run pytest

format:
	@echo '==> Reformatting files'
	@poetry run black -q owid/walden/
	@poetry run black -q ingests/
	@poetry run python owid/walden/dev/format_json.py

watch:
	poetry run watchmedo shell-command -c 'clear; make test' --recursive --drop .
