import subprocess
import sys

def test_cli_exposes_reproducible_commands():
    result = subprocess.run([sys.executable,"-m","nyc311.cli","--help"],capture_output=True,text=True)
    assert result.returncode == 0
    for command in ["fetch-sample","etl","analyze","stream-demo","bench","report","predict"]:
        assert command in result.stdout


def test_cli_returns_nonzero_for_persisted_model_failure(monkeypatch):
    from nyc311 import cli
    monkeypatch.setattr(cli,"dispatch",lambda args,cfg:{"status":"Failed","reason":"test"})
    monkeypatch.setattr(cli,"log_event",lambda *args,**kwargs:None)
    assert cli.main(["report"]) == 1
