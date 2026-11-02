"""Isolated offline engineering proof; synthetic outputs never enter science."""
import os
import time
from pathlib import Path
from .config import ROOT, spark_session, write_json

def smoke(config):
    from .etl.build_silver import build_silver
    from .analysis.survival import build_labels
    from .streaming.consumer import stream_demo
    import pandas as pd
    output = ROOT / "data/smoke"
    output.mkdir(parents=True,exist_ok=True)
    started = time.perf_counter()
    result = {"source_kind":"synthetic_fixture","statistical_evidence":False}
    spark = spark_session("nyc311-offline-smoke")
    try:
        result["etl"] = build_silver(ROOT / "tests/fixtures/nyc311_synthetic.jsonl",output/"silver",output/"data_quality.json",spark=spark)
        labels,metrics = build_labels(pd.read_parquet(output/"silver"),config["analysis"]["as_of"])
        result["censoring"] = metrics
        def format_predictor(event):
            return {"model_version":"fixture-format-test","predicted_median_days":None,"p_closed_7d":None,"p_closed_30d":None,"p_closed_90d":None,"prediction_status":"schema-test-only"}
        dsn = os.getenv("DATABASE_URL","postgresql://nyc311:nyc311_local@localhost:55432/nyc311")
        try:
            result["streaming"] = stream_demo(spark,output/"silver",output/"stream",dsn,predictor=format_predictor,max_tickets=30,source_kind="synthetic_fixture",as_of=config["analysis"]["as_of"])
        except Exception as e:
            result["streaming"] = {"status":"Failed","reason":str(e),"action":"Run make db-up then make smoke; see PostgreSQL logs"}
            write_json(ROOT/"artifacts/metrics/smoke.json",result)
            raise
        result["status"] = "Passed"
        result["elapsed_seconds"] = time.perf_counter()-started
        write_json(ROOT/"artifacts/metrics/smoke.json",result)
        return result
    finally:
        spark.stop()
