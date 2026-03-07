"""
env.py — Alembic environment for Compliance Gate.
Points to src/compliance_gate to pick up all ORM models.
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# ── Add project src/ to sys.path so models can be imported ──────────────────
_HERE = Path(__file__).resolve()
_SRC = _HERE.parents[4]     # src/compliance_gate/infra/db/migrations → src/
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# ── Import models so Alembic can detect them ─────────────────────────────────
from compliance_gate.infra.db.session import Base  # noqa: E402
import compliance_gate.infra.db.models  # noqa: F401 — registers all ORM classes
import compliance_gate.infra.db.models_profiles # noqa: F401

# ── Alembic config ────────────────────────────────────────────────────────────
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url with environment variable if set
_db_url = os.environ.get("DATABASE_URL", "")
if _db_url:
    config.set_main_option("sqlalchemy.url", _db_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
