"""Reconciliation checks shared by ETL tests and CLI."""


def assert_reconciliation(metrics):
    accounted = metrics["accepted_rows"] + metrics["rejected_rows"] + metrics["deduplicated_rows"]
    if metrics["input_rows"] != accounted:
        raise AssertionError(f"Row reconciliation failed: {metrics['input_rows']} != {accounted}")
    if metrics["duplicate_silver_keys"]:
        raise AssertionError("Silver unique_key is not unique")
    return True
