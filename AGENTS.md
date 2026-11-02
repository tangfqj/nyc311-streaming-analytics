# NYC311 implementation constraints

NYC311_BigData_Codex_Implementation_Plan.md is the implementation specification. User instructions take precedence.
- Progress M1 environment/ingest/ETL, M2 streaming, M3 survival/prediction, M4 weather/capacity, M5 benchmarks/report. Execute tests at each gate.
- Never fabricate results. Reports and dashboard read persisted artifacts. Mark Failed/Not Run honestly.
- NYC Open Data is a daily snapshot, not CDC. Only created/closed events are reconstructed; injected updates/duplicates/disorder are labelled.
- Keep synthetic fixtures and scale-test replicas separate from official data and scientific results.
- Use fixed as_of dates, right censoring, creation-only features and chronological train/test splits. Do not use final status as a feature.
- PostgreSQL event deduplication, monotone ticket versions, predictions, and batch commits must be atomic. Test replay after commit, restart, invalid and late events.
- Capacity simulation is assumption-bound and calibration-gated, never a staffing causal claim.
- Log counts, provenance, versions, duration, censoring and failures in machine-readable artifacts. Never commit secrets, raw data, checkpoints or virtual environments.
- Keep README, CLI, Makefile, tests and reports synchronized. Prefer a working small pipeline over added infrastructure.
