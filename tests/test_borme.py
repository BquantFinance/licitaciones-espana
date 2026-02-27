"""Tests for BORME ingest and anomaly detection module."""
from pathlib import Path
import pytest
from etl.borme import (
    get_borme_tmp_dir,
    get_borme_schema,
    anos_to_date_range,
)


def test_get_borme_tmp_dir():
    d = get_borme_tmp_dir()
    assert isinstance(d, Path)
    assert "borme" in str(d)


def test_get_borme_schema_default():
    import os
    os.environ.pop("BORME_SCHEMA", None)
    assert get_borme_schema() == "borme"


def test_anos_to_date_range():
    start, end = anos_to_date_range("2020-2023")
    assert start == "2020-01-01"
    assert end == "2023-12-31"


def test_anos_to_date_range_single_year():
    start, end = anos_to_date_range("2024")
    assert start == "2024-01-01"
    assert end == "2024-12-31"
