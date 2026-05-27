from pydantic import BaseModel
from typing import Iterable

def from_record[T:(BaseModel)](type:type[T], record) -> T:
    return type.model_validate(dict(record))


def gen_models_from_records[T:(BaseModel)](type:type[T], records:Iterable) -> Iterable[T]:
    for record in records:
        yield from_record(type, record)