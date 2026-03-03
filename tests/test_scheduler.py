"""Tests for scheduler next-run logic (get_next_run_at)."""
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from etl.scheduler import SCHEDULER_TZ, get_next_run_at


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
