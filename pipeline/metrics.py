"""Tiny run-metrics recorder: per-stage wall time + row counts -> data/metrics/latest.json."""
from __future__ import annotations

import json
import time
from contextlib import contextmanager

from . import config


def _path():
    config.METRICS.mkdir(parents=True, exist_ok=True)
    return config.METRICS / "latest.json"


def load() -> dict:
    p = _path()
    return json.loads(p.read_text()) if p.exists() else {}


def record(stage: str, **values):
    data = load()
    data.setdefault(stage, {}).update(values)
    _path().write_text(json.dumps(data, indent=2, default=str))


@contextmanager
def timed(stage: str):
    t0 = time.time()
    yield
    record(stage, seconds=round(time.time() - t0, 2))
