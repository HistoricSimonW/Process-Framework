import logging
from contextlib import contextmanager
from collections.abc import Iterable

@contextmanager
def suppress_logging(
    names: Iterable[str],
    level: int = logging.WARNING,
):
    """Temporarily raise logger levels for the named loggers."""

    loggers = [logging.getLogger(name) for name in names]
    previous = [(logger, logger.level) for logger in loggers]

    try:
        for logger in loggers:
            logger.setLevel(level)
        yield
    finally:
        for logger, old_level in previous:
            logger.setLevel(old_level)