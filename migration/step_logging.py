"""
Console-friendly step logging during per-project tqdm progress.

Lives in its own module to avoid circular imports (``migration.progress`` loads
``migration.extract``, which must not import ``progress`` during package init).
"""
from __future__ import annotations

import logging
import threading
from typing import Any

_migration_console_quiet = threading.local()


def set_migration_progress_console_quiet(active: bool) -> None:
    """
    When True, repetitive migration step logs use DEBUG so tqdm can own the console
    (thread-local for parallel per-project workers).
    """
    _migration_console_quiet.active = bool(active)


def migration_progress_console_quiet() -> bool:
    return bool(getattr(_migration_console_quiet, "active", False))


def step_log_info(
    log: logging.Logger, msg: str, *args: Any, **kwargs: Any
) -> None:
    """INFO, or DEBUG while a project progress bar is active."""
    if migration_progress_console_quiet():
        log.debug(msg, *args, **kwargs)
    else:
        log.info(msg, *args, **kwargs)
