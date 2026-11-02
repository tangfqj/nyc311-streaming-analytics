"""Reproduce dashboard control checks against the locally persisted cohort.

Run: .venv/bin/python dashboard/verify_interactions.py
Browser pointer/hover/brush rendering is a separate manual verification.
"""
from datetime import date
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streamlit.testing.v1 import AppTest
from dashboard.explorer_ui import _date_bound
from nyc311.analysis.explorer import filter_requests, load_explorer_inputs
from nyc311.config import ROOT


def run():
    started = time.perf_counter()
    result = {"status": "Running", "command": ".venv/bin/python dashboard/verify_interactions.py",
              "script": "dashboard/verify_interactions.py", "source": "persisted official silver and source manifest",
              "timezone": "America/New_York creation-calendar dates; chart calendar encoded on UTC scale",
              "as_of": "2025-07-01T00:00:00Z", "checks": [],
              "scope": "Streamlit AppTest control and data assertions; not a browser screenshot or pointer test",
              "browser_brush": "Not Run by this script; requires actual browser pointer verification"}
    frame, coverage = load_explorer_inputs()
    result["source_rows"] = len(frame)
    result["downloaded_dates"] = [coverage["start_date"], coverage["end_date_exclusive"]]
    at = AppTest.from_file(str(ROOT / "dashboard/app.py"), default_timeout=90).run()

    def control(nodes, label):
        return next(node for node in nodes if node.label == label)

    def check(name, expected_rows=None):
        assert not at.exception, [error.message for error in at.exception]
        metrics = {metric.label: metric.value for metric in at.metric}
        if expected_rows is not None:
            label = "Selected survival cohort" if "Selected survival cohort" in metrics else "Selected sample requests"
            assert int(metrics[label].replace(",", "")) == expected_rows, (name, metrics, expected_rows)
        result["checks"].append({"name": name, "status": "Passed", "metrics": metrics})

    try:
        check("Overview initial official cohort", len(frame))
        control(at.date_input, "Created on or after").set_value(date(2025, 2, 1))
        control(at.date_input, "Created on or before").set_value(date(2025, 2, 28)).run()
        feb = filter_requests(frame, "2025-02-01", "2025-02-28")
        check("Inclusive February creation window", len(feb))
        assert any(f"{len(feb):,}" in node.value and "downloaded systematic sample" in node.value for node in at.markdown)
        result["checks"].append({"name": "Dynamic narrative includes filtered February cohort", "status": "Passed", "selected_rows": len(feb)})
        control(at.selectbox, "Chart aggregation").set_value("Month").run()
        check("Monthly aggregation preserves cohort", len(feb))
        control(at.checkbox, "Compare previous equal-length period").check().run()
        check("Previous-period comparison enabled", len(feb))
        control(at.checkbox, "All agencies").uncheck().run()
        check("Explicit empty category selection means no requests", 0)
        control(at.multiselect, "Agencies").set_value(["HPD"]).run()
        feb_hpd = filter_requests(frame, "2025-02-01", "2025-02-28", agencies=["HPD"])
        check("Agency selection intersects date cohort", len(feb_hpd))
        at.radio[0].set_value("Survival & Prediction").run()
        control(at.checkbox, "Show 95% confidence intervals").check().run()
        check("Filtered Kaplan–Meier with confidence interval", len(feb_hpd))
        control(at.selectbox, "Compare by").set_value("Borough").run()
        check("Filtered Kaplan–Meier borough grouping", len(feb_hpd))
        at.radio[0].set_value("Seasonality & Capacity").run()
        control(at.selectbox, "Weather variable").set_value("Precipitation").run()
        check("Filtered weather and selected weather variable", len(feb_hpd))
        at.radio[0].set_value("Overview").run()
        control(at.selectbox, "Date selection").set_value("On or before date").run()
        control(at.date_input, "Created on or before · inclusive").set_value(date(2025, 1, 15)).run()
        jan_hpd = filter_requests(frame, "2025-01-01", "2025-01-15", agencies=["HPD"])
        check("Inclusive before-date mode", len(jan_hpd))
        control(at.selectbox, "Available year").set_value(2025).run()
        check("Available-year selection remains valid")
        at.radio[0].set_value("Scalability").run()
        check("Persisted benchmark page independent of cohort filters")
        assert _date_bound(1735689600000) == date(2025, 1, 1)
        assert _date_bound("2025-01-01T00:00:00Z") == date(2025, 1, 1)
        result["checks"].append({"name": "UTC-midnight brush values preserve calendar day", "status": "Passed",
                                 "epoch_ms": 1735689600000, "expected_calendar_date": "2025-01-01"})
        result["status"] = "Passed"
    except Exception as exc:
        result.update({"status": "Failed", "reason": f"{type(exc).__name__}: {exc}"})
        raise
    finally:
        result["duration_seconds"] = time.perf_counter() - started
        result["passed_checks"] = sum(item["status"] == "Passed" for item in result["checks"])
        output = ROOT / "artifacts/metrics/interactive_dashboard_app_test.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"status": result["status"], "passed_checks": result["passed_checks"], "output": str(output)}))


if __name__ == "__main__":
    run()
