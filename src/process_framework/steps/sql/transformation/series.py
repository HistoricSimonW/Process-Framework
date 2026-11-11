from typing import Any, Callable
from functools import partial

from pandas import Series


type SeriesTransformer[T] = Callable[[Series], T]


def groupby_and_agg(s: Series, agg: Any, level: int | str | None = None, by: Any = None) -> Series:
    """Group a Series and aggregate its values."""
    return s.groupby(by=by, level=level).agg(agg)


def as_agged_groupby(agg: Any, level: int | str | None = None, by: Any = None) -> SeriesTransformer[Series]:
    """Return a Series transformer that groups and aggregates values."""
    return partial(
        groupby_and_agg,
        agg=agg,
        level=level,
        by=by,
    )
    