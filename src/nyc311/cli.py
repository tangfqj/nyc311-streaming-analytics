"""Independent reproducible entry points; see Makefile for defaults."""
import argparse
import json
import os
from pathlib import Path
import time
import traceback
from .config import ROOT, load_config, log_event, spark_session, write_json


def main(argv=None):
    parser = argparse.ArgumentParser(prog="nyc311")
    parser.add_argument("--config", default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("doctor")
    fetch = sub.add_parser("fetch-sample")
    fetch.add_argument("--start"); fetch.add_argument("--end")
    fetch.add_argument("--max-rows",type=int); fetch.add_argument("--output",default="data/raw/nyc311_12mo_full")
    fetch.add_argument("--sample-suffix", default="none",
                       help="Numeric unique_key suffix for a systematic subsample; 'none' requests the full population")
    etl = sub.add_parser("etl")
    etl.add_argument("--input",default="data/raw/nyc311_12mo_full")
    etl.add_argument("--output",default="data/silver")
    etl.add_argument("--metrics",default="artifacts/metrics/data_quality.json")
    analyze = sub.add_parser("analyze")
    analyze.add_argument("--silver",default="data/silver")
    analyze.add_argument("--output",default="artifacts")
    analyze.add_argument("--part", choices=["all","survival","model","seasonality","capacity"],default="all")
    weather = sub.add_parser("fetch-weather")
    weather.add_argument("--start",default="2025-11-01"); weather.add_argument("--end",default="2026-11-01")
    weather.add_argument("--output",default="data/weather")
    stream = sub.add_parser("stream-demo")
    stream.add_argument("--silver",default="data/silver")
    stream.add_argument("--output",default="data/stream-demo")
    stream.add_argument("--max-tickets",type=int,default=100)
    producer = sub.add_parser("produce")
    producer.add_argument("--silver",default="data/silver"); producer.add_argument("--incoming",default="data/live/incoming")
    producer.add_argument("--batch-size",type=int,default=100); producer.add_argument("--speed",type=float,default=0)
    producer.add_argument("--max-events",type=int,default=1000); producer.add_argument("--seed",type=int,default=311)
    producer.add_argument("--duplicate-fraction",type=float,default=0.1); producer.add_argument("--disorder-fraction",type=float,default=0.1)
    consumer = sub.add_parser("consume")
    consumer.add_argument("--incoming",default="data/live/incoming"); consumer.add_argument("--checkpoint",default="data/live/checkpoint")
    consumer.add_argument("--pipeline-id",default="nyc311-live-v1"); consumer.add_argument("--continuous",action="store_true")
    predict = sub.add_parser("predict")
    predict.add_argument("--agency",required=True); predict.add_argument("--complaint-type",required=True)
    predict.add_argument("--borough",required=True); predict.add_argument("--created-at",required=True)
    predict.add_argument("--model",default="artifacts/models/cox_model.pkl")
    bench = sub.add_parser("bench")
    bench.add_argument("--input",default="data/raw/nyc311_12mo_full")
    bench.add_argument("--output",default="artifacts/benchmark/local")
    bench.add_argument("--multipliers",default="1,10,100"); bench.add_argument("--repetitions",type=int,default=3)
    bench.add_argument("--base-rows",type=int,default=1000); bench.add_argument("--master",default="local[2]")
    plots = sub.add_parser("bench-plots", help="Rebuild applicable benchmark charts from saved run records")
    plots.add_argument("--root", default="artifacts/benchmark")
    sub.add_parser("smoke"); sub.add_parser("report")
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    started = time.perf_counter()
    log_event("cli",command=args.command,status="Started")
    try:
        result = dispatch(args,cfg)
        if isinstance(result, dict) and (result.get("status") == "Failed" or result.get("run_status") == "Failed"):
            raise RuntimeError(f"{args.command} failed; inspect persisted metrics")
        if args.command == "bench" and isinstance(result, list) and any(row.get("failed_runs",0) or row.get("not_run",0) for row in result):
            raise RuntimeError("Benchmark contains Failed/Not Run attempts; inspect runs.csv and summary.json")
        log_event("cli",command=args.command,status="Passed",elapsed_seconds=time.perf_counter()-started)
        print(json.dumps(result,indent=2,default=str,allow_nan=False))
        return 0
    except Exception as exc:
        log_event("cli",command=args.command,status="Failed",reason=str(exc),elapsed_seconds=time.perf_counter()-started)
        traceback.print_exc()
        return 1


def dispatch(a,cfg):
    if a.command == "doctor":
        from .doctor import run_doctor
        return run_doctor()
    if a.command == "fetch-sample":
        from .ingest.download_311 import download_311
        result = download_311(a.output,start_date=a.start or cfg["ingest"]["start"],end_date=a.end or cfg["ingest"]["end"],max_rows=a.max_rows or cfg["ingest"]["max_rows"],page_size=cfg["ingest"]["page_size"],window_days=cfg["ingest"]["window_days"],sample_suffix=(None if str(a.sample_suffix).strip().lower() in ("", "none", "null") else a.sample_suffix))
        write_json("artifacts/metrics/source_manifest.json", result)
        return result
    if a.command == "etl":
        from .etl.build_silver import build_silver
        return build_silver(a.input,a.output,a.metrics)
    if a.command == "fetch-weather":
        from .ingest.download_weather import download_weather
        return download_weather(a.output,a.start,a.end)
    if a.command == "analyze":
        from .analysis.survival import run_survival
        from .analysis.train_cox import train_cox
        from .analysis.seasonality import run_seasonality
        from .analysis.capacity import run_capacity
        result = {}
        for name,fn in [("survival",run_survival),("model",train_cox),("seasonality",run_seasonality),("capacity",run_capacity)]:
            if a.part in (name,"all"):
                kw = {"config":cfg}
                if name == "seasonality": kw["weather_path"] = "data/weather"
                result[name] = fn(a.silver,a.output,**kw)
                if result[name].get("status") == "Failed":
                    raise RuntimeError(f"Analysis {name} failed; inspect persisted metrics")
        return result
    if a.command == "predict":
        from .analysis.predict import predict_record
        return predict_record({"agency":a.agency,"complaint_type":a.complaint_type,"borough":a.borough,"created_at":a.created_at}, a.model)
    if a.command in ("stream-demo","consume"):
        from .streaming.consumer import stream_demo,run_stream
        from .analysis.predict import predict_record
        model = Path("artifacts/models/cox_model.pkl")
        predictor = (lambda event: predict_record(event,model)) if model.exists() else None
        spark = spark_session("nyc311-stream")
        try:
            dsn = os.environ.get("DATABASE_URL", "postgresql://nyc311:nyc311_local@localhost:55432/nyc311")
            if a.command == "stream-demo":
                result = stream_demo(spark,a.silver,a.output,dsn,predictor=predictor,max_tickets=a.max_tickets,as_of=cfg["analysis"]["as_of"],min_created_at=cfg["analysis"]["train_end"])
                write_json("artifacts/metrics/streaming_metrics.json",result)
                return result
            query = run_stream(spark,a.incoming,a.checkpoint,dsn,pipeline_id=a.pipeline_id,predictor=predictor,available_now=not a.continuous)
            if a.continuous:
                query.awaitTermination()
                return {"status":"Stopped","pipeline_id":a.pipeline_id}
            return query
        finally: spark.stop()
    if a.command == "produce":
        import pandas as pd
        from .streaming.event_builder import reconstruct_events
        from .streaming.producer import produce
        records = pd.read_parquet(a.silver).head(a.max_events).to_dict("records")
        events = reconstruct_events(records,as_of=cfg["analysis"]["as_of"])
        return produce(events,a.incoming,batch_size=a.batch_size,speed=a.speed,max_events=a.max_events,seed=a.seed,duplicate_fraction=a.duplicate_fraction,disorder_fraction=a.disorder_fraction)
    if a.command == "bench":
        from .benchmark.run_benchmark import run_benchmark
        return run_benchmark(a.input,a.output,multipliers=tuple(map(int,a.multipliers.split(","))),repetitions=a.repetitions,master=a.master,base_rows=a.base_rows)
    if a.command == "bench-plots":
        from .benchmark.plot_scaling import plot_scaling
        folders = sorted({path.parent for path in Path(a.root).rglob("runs.csv")})
        if not folders:
            raise FileNotFoundError(f"No benchmark run records found under {a.root}")
        return {"status": "Passed", "source": "persisted benchmark run records",
                "directories": [{"path": str(folder), "figures": plot_scaling(folder)} for folder in folders]}
    if a.command == "smoke":
        from .smoke import smoke
        return smoke(cfg)
    if a.command == "report":
        from .report import build_report
        return build_report()
    raise ValueError(a.command)

if __name__ == "__main__":
    raise SystemExit(main())
