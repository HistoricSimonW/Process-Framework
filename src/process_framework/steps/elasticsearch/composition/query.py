from process_framework import ValueOrReference, resolve
from typing import Sequence
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Query(ABC):
    """Base class for Elasticsearch query fragments."""
    @abstractmethod
    def get_query(self) -> dict:
        ...


@dataclass(kw_only=True)
class ValuesQuery(Query, ABC):
    """Base class for queries that operate on a sequence of values."""
    values:ValueOrReference[Sequence]|Sequence

    def get_values(self) -> list:
        values = resolve(self.values)
        return list(values)


@dataclass(kw_only=True)
class MatchAll(Query):
    """Match all documents."""
    def get_query(self) -> dict:
        return {'match_all':{}}


@dataclass(kw_only=True)
class Ids(ValuesQuery):
    """Match documents by Elasticsearch document id."""
    def get_query(self) -> dict:
        return {
            'ids':{'values':self.get_values()}
        }

@dataclass(kw_only=True)
class Terms(ValuesQuery):
    """Match documents whose field contains one of the supplied values."""
    field:str

    def get_query(self) -> dict:
        return {
            'terms':{
                self.field:self.get_values()
            }
        }
