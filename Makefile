# Compliance Gate - Automation Makefile

.PHONY: help install-backend install-frontend install dev-backend dev-frontend dev stop clean

# Default help command
help:
	@echo "Compliance Gate - Available commands:"
	@echo "  make install         - Install both backend and frontend dependencies"
	@echo "  make dev             - Run both backend and frontend (parallel)"
	@echo "  make dev-backend     - Run only the FastAPI backend"
	@echo "  make dev-frontend    - Run only the Vite frontend"
	@echo "  make stop            - Stop all running backend and frontend processes"
	@echo "  make clean           - Remove temporary files, caches and node_modules"
	@echo "  make verify          - Run the Python verification script"

# --- Installation ---

install-backend:
	@echo "Installing backend dependencies..."
	python3 -m venv .venv && source .venv/bin/activate && pip install -e .

install-frontend:
	@echo "Installing frontend dependencies..."
	cd frontend && npm install

install: install-backend install-frontend
	@echo "Environment ready."

# --- Development ---

dev-backend:
	@echo "Starting Backend (Port 8000)..."
	export CG_DATA_DIR=$$(pwd) && source .venv/bin/activate && uvicorn compliance_gate.main:app --host 0.0.0.0 --port 8000 --reload

dev-frontend:
	@echo "Starting Frontend (Port 3000)..."
	cd frontend && npm run dev -- --host 0.0.0.0 --port 3000

dev:
	@make -j 2 dev-backend dev-frontend

# --- Process Management ---

stop:
	@echo "Stopping backend and frontend processes..."
	@pkill -f "uvicorn compliance_gate.main:app" || true
	@pkill -f "vite" || true
	@echo "Processes stopped."

# --- Utilities ---

verify:
	@echo "Running environment verification..."
	python3 scripts/verify_env.py

clean:
	@echo "Cleaning environment..."
	rm -rf .venv
	rm -rf frontend/node_modules
	rm -rf frontend/dist
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Clean complete."
