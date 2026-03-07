import os
import sys

sys.path.insert(0, os.path.abspath("src"))

# Import models for side effects so SQLAlchemy metadata is fully registered.
import compliance_gate.authentication.models  # noqa: F401
import compliance_gate.infra.db.models  # noqa: F401
import compliance_gate.infra.db.models_engine  # noqa: F401
import compliance_gate.infra.db.models_profiles  # noqa: F401
from compliance_gate.infra.db.session import Base, engine

print("Creating tables in Postgres...")
Base.metadata.create_all(bind=engine)
print("Done.")
