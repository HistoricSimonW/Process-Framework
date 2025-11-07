from process_framework.references.reference import Reference
from process_framework.steps import AssigningStep
from pandas import Series, Index, DataFrame, MultiIndex
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from typing import Iterable, Any
import logging

class GetElasticDocumentVersions(AssigningStep[Index]):
    """ scan an elasticsearch index, producing a `MultiIndex` of `fields`, which can be compared with another `MultiIndex` to detect changes """
    def __init__(self, assign_to: Reference[Index], elasticsearch:Elasticsearch, 
                 index:str,
                 *fields:tuple[str, str|type]|str,
                 default_field_type:str|type = str,
                 include_id:bool=True,
                 overwrite:bool=True):

        super().__init__(assign_to, overwrite=overwrite)
        self.elasticsearch = elasticsearch
        self.index = index
        self.fields = fields
        self.default_field_type=default_field_type
        self.include_id=include_id


    def get_source(self) -> list[str]:
        """ get a source list of [field] that can be passed to elasticsearch.scan(source=source) """
        source = []
        for field in self.fields:
            if isinstance(field, tuple) and isinstance(field[0], str):
                source.append(field[0])
            elif isinstance(field, str):
                source.append(field)
            else:
                raise ValueError(f'expected `field` to be `str` or `tuple[str,str|type], got {field}')
        return source


    def get_astype(self) -> dict:
        """ get a dict of {field:type} that can be passed to DataFrame.astype() """
        astype = dict()
        for field in self.fields:
            if isinstance(field, tuple) and isinstance(field[0], str) and isinstance(field[1], (str, type)):
                astype[field[0]] = field[1]
            elif isinstance(field, str):
                astype[field] = self.default_field_type
            else:
                raise ValueError(f'expected `field` to be `str` or `tuple[str,str|type], got {field}')
        return astype
    

    def generate(self) -> Index:
        if self.assign_to.has_value() and not self.overwrite:
            logging.info(f'`assign_to` has value and `{self}` has `overwrite` flag set to False, returning current value')
            return self.assign_to.get_value()
        
        source = self.get_source()

        scan_:Iterable[dict[str, Any]] = scan(
            client=self.elasticsearch,
            index=self.index,
            source=source
        )

        return self.index_from_scan_(scan_)
    

    def index_from_scan_(self, scan_) -> Index:

        df = DataFrame.from_records(scan_) #type:ignore

        if len(df) == 0:
            return Index([])
        
        if self.include_id:
            df = df.set_index('_id')

        df = DataFrame.from_records(df._source.values, index=df.index) #type:ignore

        df = df.astype(self.get_astype()).reset_index()

        index_keys = (['_id'] if self.include_id else []) + self.get_source() 

        return df.set_index(index_keys).index