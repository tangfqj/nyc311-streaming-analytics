"""Persist environment evidence without exposing credentials."""
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path
import psutil
import requests
from .config import ROOT, write_json

def run_doctor(network=True):
    def command(args):
        try:
            p = subprocess.run(args, capture_output=True, text=True, timeout=20)
            return {"status": "Passed" if p.returncode == 0 else "Failed", "returncode": p.returncode,
                    "output": (p.stdout + p.stderr).strip()[:2500]}
        except Exception as e:
            return {"status": "Failed", "reason": str(e)}
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.getenv("JAVA_HOME") else "java"
    out = {"python": sys.version, "python_executable": sys.executable,
           "platform": platform.platform(), "machine": platform.machine(),
           "logical_cpu_count": psutil.cpu_count(), "ram_bytes": psutil.virtual_memory().total,
           "available_ram_bytes": psutil.virtual_memory().available,
           "disk_free_bytes": shutil.disk_usage(ROOT).free,
           "java": command([java, "-version"]),
           "docker": command(["docker", "info", "--format", "{{.ServerVersion}} CPU={{.NCPU}} RAM={{.MemTotal}}"]),
           "optional_app_token_present": bool(os.getenv("SOCRATA_APP_TOKEN")),
           "database_url_configured": bool(os.getenv("DATABASE_URL"))}
    if network:
        try:
            response = requests.get("https://data.cityofnewyork.us/api/views/erm2-nwe9.json", timeout=20)
            response.raise_for_status()
            metadata = response.json()
            fields = [c["fieldName"] for c in metadata["columns"]]
            out["network"] = {"status": "Passed", "dataset": metadata.get("name"),
                              "complaint_type_verified": "complaint_type" in fields,
                              "selected_field_names": [c for c in fields if c in ["unique_key","created_date","closed_date","complaint_type","agency","borough"]]}
        except Exception as e:
            out["network"] = {"status": "Failed", "reason": str(e)}
    out["mode"] = "local Spark + PostgreSQL; Docker standalone optional" if out["docker"]["status"] == "Passed" else "local Spark; PostgreSQL requires running service"
    write_json(ROOT / "artifacts/metrics/environment.json", out)
    return out
