PYTHON=python
PIP=$(PYTHON) -m pip

install:
	$(PIP) install -e .[test]

uninstall:
	$(PIP) uninstall crunch-convert

test:
	$(PYTHON) -m pytest -vv

test-with-coverage:
	$(PYTHON) -m pytest --cov=crunch_global_leaderboard --cov-report=html -vv

build:
	rm -rf build *.egg-info dist
	python setup.py sdist bdist_wheel

.PHONY: install uninstall test build
