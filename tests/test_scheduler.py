"""Tests for scheduler next-run logic (get_next_run_at)."""
import os
from datetime import datetime
import pytest

from etl.scheduler import SCHEDULER_TZ, VALID_SCHEDULE_EXPRS, get_next_run_at, validate_schedule_expr


def test_next_run_trimestral_after_last_finish_same_day():
    """Next run is first slot *after* last_finished_at, not after 'now'.
    If last run finished Jan 1 00:30 Madrid, next run must be Jan 1 02:00, not Apr 1."""
    # Last run finished just after midnight on Jan 1 2026 (Madrid)
    last = datetime(2026, 1, 1, 0, 30, 0, tzinfo=SCHEDULER_TZ)
    # "Now" is later that day so the slot Jan 1 02:00 is in the past
    now = datetime(2026, 1, 1, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    next_at = get_next_run_at("Trimestral", last, reference_now=now)
    # Must be Jan 1 02:00 (the due slot we missed), not Apr 1
    assert next_at.year == 2026 and next_at.month == 1 and next_at.day == 1
    assert next_at.hour == 2 and next_at.minute == 0


def test_next_run_mensual_after_last_finish_same_day():
    """Mensual: if last run finished Mar 1 01:00, next run is Mar 1 02:00."""
    last = datetime(2026, 3, 1, 1, 0, 0, tzinfo=SCHEDULER_TZ)
    now = datetime(2026, 3, 2, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    next_at = get_next_run_at("Mensual", last, reference_now=now)
    assert next_at.year == 2026 and next_at.month == 3 and next_at.day == 1
    assert next_at.hour == 2


def test_next_run_trimestral_oct_finish_returns_jan():
    """Last run Oct 2025; next run is Jan 1 2026 02:00."""
    last = datetime(2025, 10, 15, 12, 0, 0, tzinfo=SCHEDULER_TZ)
    now = datetime(2026, 3, 2, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    next_at = get_next_run_at("Trimestral", last, reference_now=now)
    assert next_at.year == 2026 and next_at.month == 1 and next_at.day == 1
    assert next_at.hour == 2


def test_next_run_none_returns_now():
    """No previous run: next run is now (task due immediately)."""
    now = datetime(2026, 3, 2, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    next_at = get_next_run_at("Trimestral", None, reference_now=now)
    assert next_at == now


def test_valid_schedule_exprs_has_six_values():
    assert len(VALID_SCHEDULE_EXPRS) == 6
    assert "Diario" in VALID_SCHEDULE_EXPRS
    assert "Semanal" in VALID_SCHEDULE_EXPRS
    assert "Mensual" in VALID_SCHEDULE_EXPRS
    assert "Trimestral" in VALID_SCHEDULE_EXPRS
    assert "Semestral" in VALID_SCHEDULE_EXPRS
    assert "Anual" in VALID_SCHEDULE_EXPRS


def test_validate_schedule_expr_valid():
    for expr in VALID_SCHEDULE_EXPRS:
        assert validate_schedule_expr(expr) == expr


def test_validate_schedule_expr_invalid():
    with pytest.raises(ValueError, match="Frecuencia no válida"):
        validate_schedule_expr("Bimensual")


def test_validate_schedule_expr_none_returns_default():
    assert validate_schedule_expr(None, default="Trimestral") == "Trimestral"


def test_next_run_diario_after_finish_before_hour():
    """Finished at 01:00 on same day → same day 02:00."""
    last = datetime(2026, 3, 25, 1, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Diario", last, reference_now=datetime(2026, 3, 25, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 3, 25, 2, 0, 0, tzinfo=SCHEDULER_TZ)

def test_next_run_diario_after_finish_after_hour():
    """Finished at 03:00 → next day 02:00."""
    last = datetime(2026, 3, 25, 3, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Diario", last, reference_now=datetime(2026, 3, 25, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 3, 26, 2, 0, 0, tzinfo=SCHEDULER_TZ)

def test_next_run_diario_none_returns_now():
    """No last run → returns reference_now (task is immediately due)."""
    now = datetime(2026, 3, 25, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Diario", None, reference_now=now)
    assert result == now

def test_next_run_diario_finish_at_exact_hour():
    """Finished exactly at 02:00 → next day 02:00 (slot not repeated same day)."""
    last = datetime(2026, 3, 25, 2, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Diario", last, reference_now=datetime(2026, 3, 25, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 3, 26, 2, 0, 0, tzinfo=SCHEDULER_TZ)


def test_next_run_semanal_wednesday_to_monday():
    """Finished Wednesday → next Monday 02:00."""
    last = datetime(2026, 3, 25, 10, 0, 0, tzinfo=SCHEDULER_TZ)  # Wednesday
    result = get_next_run_at("Semanal", last, reference_now=datetime(2026, 3, 26, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 3, 30, 2, 0, 0, tzinfo=SCHEDULER_TZ)  # Monday

def test_next_run_semanal_monday_before_hour():
    """Finished Monday at 01:00 → same Monday 02:00."""
    last = datetime(2026, 3, 30, 1, 0, 0, tzinfo=SCHEDULER_TZ)  # Monday 01:00
    result = get_next_run_at("Semanal", last, reference_now=datetime(2026, 3, 30, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 3, 30, 2, 0, 0, tzinfo=SCHEDULER_TZ)

def test_next_run_semanal_monday_after_hour():
    """Finished Monday at 03:00 → next Monday 02:00."""
    last = datetime(2026, 3, 30, 3, 0, 0, tzinfo=SCHEDULER_TZ)  # Monday 03:00
    result = get_next_run_at("Semanal", last, reference_now=datetime(2026, 3, 30, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 4, 6, 2, 0, 0, tzinfo=SCHEDULER_TZ)  # Next Monday

def test_next_run_semanal_finish_at_exact_hour_monday():
    """Finished Monday exactly at 02:00 → next Monday 02:00."""
    last = datetime(2026, 3, 30, 2, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Semanal", last, reference_now=datetime(2026, 3, 30, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 4, 6, 2, 0, 0, tzinfo=SCHEDULER_TZ)


def test_next_run_semestral_march_to_july():
    """Finished March → next Jul 1 02:00."""
    last = datetime(2026, 3, 25, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Semestral", last, reference_now=datetime(2026, 3, 26, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 7, 1, 2, 0, 0, tzinfo=SCHEDULER_TZ)

def test_next_run_semestral_august_to_jan():
    """Finished August → next Jan 1 02:00."""
    last = datetime(2026, 8, 15, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Semestral", last, reference_now=datetime(2026, 8, 16, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2027, 1, 1, 2, 0, 0, tzinfo=SCHEDULER_TZ)

def test_next_run_semestral_dec_to_jan():
    """Finished December → Jan 1 next year."""
    last = datetime(2026, 12, 20, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Semestral", last, reference_now=datetime(2026, 12, 21, 10, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2027, 1, 1, 2, 0, 0, tzinfo=SCHEDULER_TZ)

def test_next_run_semestral_june_after_july():
    """Finished June → Jul 1 same year."""
    last = datetime(2026, 6, 30, 10, 0, 0, tzinfo=SCHEDULER_TZ)
    result = get_next_run_at("Semestral", last, reference_now=datetime(2026, 7, 1, 0, 0, 0, tzinfo=SCHEDULER_TZ))
    assert result == datetime(2026, 7, 1, 2, 0, 0, tzinfo=SCHEDULER_TZ)
