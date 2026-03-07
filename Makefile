# Compliance Gate - Automation Makefile

.PHONY: help install dev stop clean verify

# Default help command
help:
	@echo "Compliance Gate - Available commands:"
	@echo "  make install         - Install backend dependencies"
	@echo "  make dev             - Run the FastAPI backend"
	@echo "  make stop            - Stop running backend processes"
	@echo "  make clean           - Remove temporary files and caches"
	@echo "  make verify          - Run the Python verification script"

# --- Installation ---

install:
	@echo "Installing backend dependencies..."
	python3 -m venv .venv && source .venv/bin/activate && pip install -e .
	@echo "Environment ready."

# --- Development ---

dev:
	@echo "Starting Backend (Port 8000)..."
	export CG_DATA_DIR=$$(pwd) && source .venv/bin/activate && uvicorn compliance_gate.main:app --host 0.0.0.0 --port 8000 --reload

# --- Process Management ---

stop:
	@echo "Stopping backend processes..."
	@pkill -f "uvicorn compliance_gate.main:app" || true
	@echo "Processes stopped."

# --- Utilities ---

verify:
	@echo "Running environment verification..."
	python3 scripts/verify_env.py

clean:
	@echo "Cleaning environment..."
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Clean complete."
