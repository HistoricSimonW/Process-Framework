from ..assigning_step import AssigningStep
from ...references.reference import Reference
from pandas import DataFrame, Series
from typing import Iterable, Any
from requests import Session
from inflection import underscore

class GetSolrQueryResult[T](AssigningStep[T]):
    """ assign the result of an ElasticSearch index scan to a context """
    def __init__(self, assign_to:Reference[T], url:str, instance:str, *, type_name:str|None, fq:str|None, fields:list[str]|None=None, start:int=0, rows:int=1000, limit:int|None=None, overwrite:bool=True):
        super().__init__(assign_to, overwrite=overwrite)
        self.url = url.strip('/')
        self.instance = instance.strip('/')
        if (type_name and fq) or (not type_name and not fq):
            raise AttributeError("expected exactly one of `type_name` or `fq`")

        self.fq = fq or f'typeName:"{type_name}"'
        self.start = start
        self.rows = rows
        self.session = Session()
        self.fields = fields
        self.limit = limit

    
    def gen_record_batches(self) -> Iterable[dict]:
        params = dict(
            fq=self.fq,
            q='*:*',
            start=self.start,
            rows=self.rows
        )

        if isinstance(self.fields, list):
            params['fl'] = ','.join(['id'] + self.fields)

        # load docs from solr
        total = 0
        select_url = f'{self.url}/{self.instance}/select'
        while True:
            response = self.session.get(
                # url='/'.join((self.url, self.instance, 'select')),
                url=select_url,
                params=params
            )
            batch = response.json()['response']['docs']
            yield batch
            total += len(batch)
            if (len(batch) % params['rows'] != 0) or (self.limit and (total >= self.limit)): # type: ignore
                break
            params['start'] += params['rows'] # type: ignore

    
    def generate(self) -> T:
        batches = self.gen_record_batches()
        records = (record for batch in batches for record in batch)

        df = DataFrame.from_records(
            data=records, # type: ignore
            index='id'
        )
        
        if isinstance(self.fields, list):
            df = df[self.fields]
        
        if 'path' in df.columns:
            df.path = df.path.str[0]

        df.columns = df.columns.map(underscore)

        if len(df.columns) == 1 and self.assign_to._type == Series:
            return df[df.columns[0]] # type: ignore
        
        if self.assign_to._type == DataFrame:
            return df # type: ignore
        
        raise ValueError("unhandled state")
    

    def preflight(self):
        self.session.head(url=self.url).raise_for_status()
        self.session.head(url=f'{self.url}/{self.instance}/select?q=*:*').raise_for_status()