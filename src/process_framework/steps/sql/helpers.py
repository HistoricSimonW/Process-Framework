from contextlib import contextmanager
from sqlalchemy import Connection, Table, ColumnElement, select, Column, MetaData
from typing import Iterable, Generator, Any
import logging
from process_framework import ValueOrReference, resolve
from uuid import uuid4
from functools import partial
from itertools import batched

logger = logging.getLogger(__name__)


def batched_records(values: Iterable[Any], size: int = 1000) -> Iterable[list[dict[str, Any]]]:
    """Yield batches of values as SQLAlchemy parameter dictionaries, handling NumPy scalar values."""

    for batch in batched(values, size):
        yield [
            {"value": value.item() if hasattr(value, "item") else value}
            for value in batch
        ]
        
        

@contextmanager
def values_temp_table(
        conn: Connection,
        values: ValueOrReference[Iterable[Any]],
        in_col: ColumnElement[Any],
    ) -> Generator[Table, None, None]:

    values = list(resolve(values))

    # Unique name prevents collisions on the same connection
    table = Table(
        f"#Temp_{uuid4().hex[:12]}",
        MetaData(),
        Column("value", in_col.type),
    )

    logger.debug("Creating temp table %s", table.name)
    table.create(conn)
    
    try:    
        if values:
            for batch in batched_records(values):
                conn.execute(table.insert(), batch)
            
        yield table

    finally:
        table.drop(conn, checkfirst=True)
        logger.debug("Dropped temp table %s", table.name)


@contextmanager
def where_in_temp_table(
        conn: Connection,
        values: ValueOrReference[Iterable[Any]],
        in_col: ColumnElement[Any],
    ) -> Generator[ColumnElement[bool], None, None]:

    with values_temp_table(conn, values, in_col) as table:
        yield in_col.in_(
            select(table.c.value)
        )
        

def in_temp_table(
    values: ValueOrReference[Iterable[Any]],
    in_col: ColumnElement[Any]):
    return partial(where_in_temp_table, values=values, in_col=in_col)