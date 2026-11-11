from collections.abc import Callable, Iterable, Sequence
from functools import partial
from typing import Any, Literal

from pandas import Series

type Hit = dict[str, Any]
type Hits = Iterable[Hit]

type FieldLocation = Literal["_source", "fields"]

type HitsTransformer[T] = Callable[[Hits], T]

def get_id(hit:Hit) -> str:
    """Return the Elasticsearch document ID."""
    return hit['_id']


def get_ids_set(hits:Hits) -> set[str]:
    """Return the unique document IDs from Elasticsearch hits."""
    _ids = (hit['_id'] for hit in hits)
    return set(_ids)


def get_first[T](value: Sequence[T]) -> T:
    """Return the first item in a sequence."""
    return value[0]


def first_as_type[T](value: Sequence, as_type:type[T]) -> T:
    """Convert the first item in a sequence to the specified type."""
    return as_type(get_first(value)) # type:ignore


def as_first[T](as_type: type[T]) -> Callable[[Sequence], T]:
    """Return a transformer that converts the first item to the specified type."""
    return partial(first_as_type, as_type=as_type)


def get_value_set[T](hits:Hits, field:str, source_or_fields:FieldLocation = '_source', 
                     value_transformer:Callable[[Any], T]|None=None) -> set[T]:
    """Return unique values of a field, optionally transforming each value."""
    values = (hit[source_or_fields][field] for hit in hits)
    
    if value_transformer is not None:
        values = map(value_transformer, values)
    
    return set(values)
    

def as_value_set[T](field:str, source_or_fields:FieldLocation = '_source', 
                    value_transformer:Callable[[Any], T]|None=None) -> HitsTransformer[set[T]]:
    """Return a transformer that extracts unique Elasticsearch field values."""
    return partial(
        get_value_set,
        field=field,
        source_or_fields=source_or_fields,
        value_transformer=value_transformer
    )
    

def get_series[T](hits:Hits, value_func:Callable[[Hit], T], id_func:Callable[[Hit], str]=get_id) -> Series:
    """Build a Series indexed by IDs using values extracted from hits."""
    values = dict()
    
    for hit in hits:
        _id = id_func(hit)
        value = value_func(hit)
        values[_id] = value
        
    return Series(values)


def get_value_series[T](hits:Hits, field:str, source_or_fields:FieldLocation = '_source', 
                     value_transformer:Callable[[Any], T]|None=None, id_func:Callable[[Hit], str]=get_id) -> Series:
    """Build a Series of field values indexed by document IDs."""
    
    if value_transformer is None:
        value_transformer = (lambda v: v)
    
    def value_func(hit:Hit) -> Any:
        value = hit[source_or_fields][field]
        return value_transformer(value)

    return get_series(
        hits,
        value_func=value_func,
        id_func=id_func
    )