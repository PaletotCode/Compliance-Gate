# Compliance Gate - Automation Makefile

.PHONY: help install dev backend-dev backend-up backend-down frontend-dev frontend-electron fullstack stop clean verify verify-all test auth-check

# Default help command
help:
	@echo "Compliance Gate - Available commands:"
	@echo "  make install         - Install backend dependencies"
	@echo "  make backend-dev     - Run backend with local .venv (uvicorn)"
	@echo "  make backend-up      - Start backend stack via docker compose (db/redis/api)"
	@echo "  make backend-down    - Stop backend stack via docker compose"
	@echo "  make frontend-dev    - Run frontend Vite dev server"
	@echo "  make frontend-electron - Run frontend inside Electron (desktop app)"
	@echo "  make fullstack       - Start backend stack and then frontend dev server"
	@echo "  make test            - Run backend + frontend tests"
	@echo "  make auth-check      - Run frontend auth flow checker"
	@echo "  make verify-all      - Run full backend+frontend integration gate"
	@echo "  make stop            - Stop backend + frontend dev processes"
	@echo "  make clean           - Remove temporary files and caches"
	@echo "  make verify          - Run the Python verification script"

# --- Installation ---

install:
	@echo "Installing backend dependencies..."
	python3 -m venv .venv
	.venv/bin/pip install -e .
	@echo "Environment ready."

# --- Development ---

dev: backend-dev

backend-dev:
	@echo "Starting Backend (Port 8000)..."
	CG_DATA_DIR=$$(pwd) .venv/bin/python -m uvicorn compliance_gate.main:app --host 0.0.0.0 --port 8000 --reload

backend-up:
	@echo "Starting backend stack (db, redis, api)..."
	docker compose up -d --build db redis api

backend-down:
	@echo "Stopping backend stack..."
	docker compose down -v --remove-orphans

frontend-dev:
	@echo "Starting Frontend (Port 5173)..."
	cd frontend && npm run dev -- --host 0.0.0.0 --port 5173

frontend-electron:
	@echo "Starting Frontend inside Electron..."
	cd frontend && npm run electron:dev

fullstack:
	@$(MAKE) backend-up
	@echo "Backend up. Starting frontend dev server..."
	cd frontend && npm run dev -- --host 0.0.0.0 --port 5173

# --- Process Management ---

stop:
	@echo "Stopping backend/frontend processes..."
	@pkill -f "uvicorn compliance_gate.main:app" || true
	@pkill -f "vite --configLoader runner" || true
	@pkill -f "electron .*frontend" || true
	@docker compose down -v --remove-orphans || true
	@echo "Processes stopped."

# --- Utilities ---

verify:
	@echo "Running environment verification..."
	python3 scripts/verify_env.py

verify-all:
	@echo "Running full integration verification..."
	bash scripts/verify_all.sh

test:
	@echo "Running backend tests..."
	.venv/bin/python -m pytest -q
	@echo "Running frontend tests..."
	cd frontend && npm test

auth-check:
	@echo "Running frontend auth flow check..."
	node frontend/scripts/auth_flow_check.ts

clean:
	@echo "Cleaning environment..."
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Clean complete."
