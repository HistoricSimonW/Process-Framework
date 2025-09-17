from process_framework.references.reference import Reference
from process_framework.steps import AssigningStep
from pandas import Series
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from typing import Iterable, Any
import logging

class GetElasticIndexState(AssigningStep[Series]):
    """ scan an elasticsearch index, producing a series of { _id : source}, where `source` is a scalar field that indicates the state of a document """
    def __init__(self, assign_to: Reference[Series], elasticsearch:Elasticsearch, index:str, source:str, dtype:str|type, *, overwrite:bool=True):
        super().__init__(assign_to)
        self.elasticsearch = elasticsearch
        self.index = index
        self.source=source
        self.dtype = dtype
        self.overwrite=overwrite


    def generate(self) -> Series:
        if self.assign_to.has_value() and not self.overwrite:
            logging.info(f'`assign_to` has value and `{self}` has `overwrite` flag set to False, returning current value')
            return self.assign_to.get_value()
        
        scan_:Iterable[dict[str, Any]] = scan(
            client=self.elasticsearch,
            index=self.index,
            source=self.source
        )

        series = Series(
            {str(doc['_id']):doc['_source'].get(self.source, None) for doc in scan_}
        ).dropna()

        if isinstance(self.dtype, (str, type)):
            series = series.astype(self.dtype) # type: ignore

        return series