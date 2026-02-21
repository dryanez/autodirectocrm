import os
import glob
from app import get_db

with get_db() as db:
    db.execute("UPDATE consignaciones SET contract_signed_at = NULL, contract_pdf = NULL WHERE id = 20")
    db.commit()
    print("Database call executed.")

for f in glob.glob("/tmp/contratos/*20*"):
    os.remove(f)
    print(f"Removed cached file: {f}")
