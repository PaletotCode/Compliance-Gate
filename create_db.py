import sys
import os

sys.path.insert(0, os.path.abspath("src"))

from compliance_gate.infra.db.session import engine, Base
# Import all models to ensure they are registered with Base
from compliance_gate.infra.db.models import *
from compliance_gate.infra.db.models_profiles import *

print("Creating tables in Postgres...")
Base.metadata.create_all(bind=engine)
print("Done.")
