"""Presentation transforms preserve calendar dates, missingness and filter context."""
import json

import numpy as np
import pandas as pd
import pytest

from dashboard.explorer_ui import _date_bound, series


def _explorer():
    return {
        "frequency": "monthly", "smoothing": 1, "as_of": "2025-07-01T00:00:00Z",
        "summary": {
            "daily": pd.DataFrame({"date": pd.to_datetime(["2025-01-30", "2025-01-31", "2025-02-01"]),
                                   "count": [2.0, np.nan, 3.0]}),
            "buckets": pd.DataFrame({"date": pd.to_datetime(["2025-01-01", "2025-02-01"]),
                "count": [np.nan, 3.0], "full_bucket": [False, False], "coverage_complete": [False, True]}),
            "selection": {"start_date": "2025-01-30", "end_date": "2025-02-01",
                          "agencies": [], "boroughs": None, "complaint_types": ["HEAT/HOT WATER"]},
        },
    }


def test_brush_calendar_dates_do_not_shift_into_previous_nyc_day():
    expected = pd.Timestamp("2025-01-01").date()
    assert _date_bound(1735689600000) == expected
    assert _date_bound("2025-01-01T00:00:00Z") == expected
    with pytest.raises(ValueError):
        _date_bound(pd.NaT)


def test_bucket_end_moving_average_preserves_missing_last_day():
    result = series(_explorer())
    assert pd.isna(result.loc[0, "moving_average"])
    assert result.loc[1, "moving_average"] == 3.0
    assert result.loc[0, "coverage"] == "Unknown extraction coverage"
    assert result.loc[1, "bucket_kind"] == "Partial calendar period"


def test_series_export_keeps_fixed_cutoff_and_exact_category_selection():
    explorer = _explorer()
    result = series(explorer)
    assert result["as_of"].eq(explorer["as_of"]).all()
    assert result["selection_end_inclusive"].eq("2025-02-01").all()
    assert result["moving_average_days"].eq(1).all()
    filters = json.loads(result.iloc[0]["selection_filters"])
    assert filters == {"agencies": [], "boroughs": None, "complaint_types": ["HEAT/HOT WATER"]}
    assert "moving_average" not in explorer["summary"]["buckets"]
