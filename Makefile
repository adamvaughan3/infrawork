PYTHON ?= python3
VENV_DIR ?= .venv
PIP := $(VENV_DIR)/bin/pip

.PHONY: venv install all

all: install

venv:
	$(PYTHON) -m venv $(VENV_DIR)
	$(PIP) install --upgrade pip

install: venv
	$(PIP) install -r requirements.txt
