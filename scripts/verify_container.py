"""Run the embedded image source in a fresh Linux container without host mounts."""
import json
import os
from pathlib import Path
import subprocess
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "nyc311-bigdata-runner"
OUT = ROOT / "artifacts/metrics/container_smoke.json"

def command(args, **kwargs):
    return subprocess.run(args,cwd=ROOT,capture_output=True,text=True,check=True,**kwargs)

def main():
    started = time.perf_counter()
    cid = None
    result = {"status":"Failed","image":IMAGE,"host_bind_mounts":False,
              "scope":"Fresh Linux application container against the existing real project PostgreSQL service; not a second physical machine"}
    try:
        info = json.loads(command(["docker","image","inspect",IMAGE]).stdout)[0]
        result.update(image_id=info["Id"],image_created=info["Created"],architecture=info["Architecture"],os=info["Os"])
        name = "nyc311-clean-check-"+uuid4().hex[:12]
        dsn = os.getenv("CONTAINER_DATABASE_URL","postgresql://nyc311:nyc311_local@postgres:5432/nyc311")
        cid = command(["docker","create","--name",name,"--network","nyc311-bigdata_default","-e","DATABASE_URL="+dsn,IMAGE]).stdout.strip()
        process = subprocess.run(["docker","start","-a",cid],cwd=ROOT,capture_output=True,text=True)
        log = ROOT/"artifacts/logs/container-smoke.log"
        log.parent.mkdir(parents=True,exist_ok=True)
        log.write_text(process.stdout+process.stderr)
        state = json.loads(command(["docker","inspect",cid]).stdout)[0]["State"]
        result.update(exit_code=state["ExitCode"],oom_killed=state["OOMKilled"],log="artifacts/logs/container-smoke.log")
        copied = ROOT/"artifacts/metrics/container_smoke_inner.json"
        command(["docker","cp",f"{cid}:/workspace/artifacts/metrics/smoke.json",str(copied)])
        inner = json.loads(copied.read_text())
        result["inner_status"] = inner.get("status")
        result["engine_versions"] = command(["docker","run","--rm",IMAGE,"python","-c","import sys,pyspark,platform;print(sys.version);print(pyspark.__version__);print(platform.platform())"]).stdout.strip()
        result["status"] = "Passed" if state["ExitCode"]==0 and inner.get("status")=="Passed" else "Failed"
    except Exception as exc:
        result["reason"] = str(exc)
        result["action"] = "Run make db-up; docker compose --profile app build runner; then make container-smoke"
    finally:
        if cid:
            subprocess.run(["docker","rm","-v",cid],capture_output=True,text=True)
        result["elapsed_seconds"] = time.perf_counter()-started
        OUT.parent.mkdir(parents=True,exist_ok=True)
        OUT.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result,indent=2))
    return 0 if result["status"]=="Passed" else 1

if __name__ == "__main__":
    raise SystemExit(main())
