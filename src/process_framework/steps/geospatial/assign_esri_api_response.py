from process_framework.references.reference import Reference
from ..transforming_step import TransformingStep
from abc import ABC, abstractmethod
from requests import Session, Response
from requests.adapters import HTTPAdapter, Retry
import urllib3
from pandas import DataFrame, Series
from geopandas import GeoDataFrame
from itertools import batched
from typing import Iterable

# this is going to end up tightly coupled to whatever process is using it.
# whatever.
class ArcGisApiTransformer(TransformingStep[DataFrame, GeoDataFrame], ABC):
    """ transform a `subject` DataFrame into a GeoDataFrame of Responses """
    def __init__(self, subject: Reference[DataFrame], assign_to: Reference[GeoDataFrame], endpoint_url:str, *, 
                 batch_size:int=1000, max_retries:int=5, retry_backoff_factor:float=10, payload_args:dict|None=None, verify_session:bool=False, overwrite:bool=True):
        super().__init__(subject, assign_to, overwrite=overwrite)
    
        self.endpoint_url = endpoint_url
        self.batch_size=batch_size
        
        self.payload_args = payload_args
        
        self.verify_session = verify_session
        self.max_retries=max_retries
        self.retry_backoff_factor=retry_backoff_factor

        self.__session__ = None
    

    def get_session(self) -> Session:
        if not isinstance(self.__session__, Session):
            self.__session__ = self.initialize_session()
        return self.__session__
    

    def initialize_session(self) -> Session:
        session = Session()
        session.verify = self.verify_session
        if not self.verify_session:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        session.mount('https://', HTTPAdapter(max_retries=self.get_retries()))
        return session
    

    def get_retries(self) -> Retry:
        """ get a `requests.Retry` object populated with `max_retries` and `retry_backoff_factor` which are optional constructor params """
        return Retry(
            total=self.max_retries,
            backoff_factor=self.retry_backoff_factor, # retry_backoff_factor=10 -> [0s, 20s, 40s, 60s, etc].
            backoff_max=120,
            allowed_methods={'post'},
            status_forcelist=[403],
            backoff_jitter=0.1
        )
    

    @staticmethod
    def gen_batches(df:DataFrame, batch_size:int) -> Iterable[DataFrame]:
        """ split a `DataFrame` into batches of `batch_size` """
        for batch in batched(df.index, batch_size):
            yield df.loc[list(batch)]
    

    def get_response_for_payload(self, session:Session, payload:dict) -> Response:
        """ POST a `payload` to the target API; check it for errors """       
        # update `payload` with `payload_args` if defined
        if isinstance(self.payload_args, dict):
            payload |= self.payload_args
        
        # get a response
        response = session.post(
            self.endpoint_url,
            data=payload
        )

        # test the response, get the json body
        response.raise_for_status()
        data = response.json()

        # if we've not hit an error, return 
        if error:= data.get('error'):
            raise Exception(error)
           
        return data


    def get_responses_for_docs(self, docs:DataFrame) -> list[Response]:
        """ for a DataFrame of `docs`, get batches of documents, get payloads for batches, and return a list of `Response` objects """
        # get or create a session
        session = self.get_session()

        # accumulate responses in a list
        responses:list[Response] = []

        for i, batch in enumerate(self.gen_batches(docs, self.batch_size)):
            payload = self.get_payload_for_batch(batch)
            try:
                response = self.get_response_for_payload(session, payload)
                responses.append(response)
            except BaseException as e:
                print(i, e)
                continue

        return responses
        

    def get_geodataframe_for_docs(self, docs:DataFrame) -> GeoDataFrame:
        """ for a dataframe of `docs`, get responses from the target API and return a `GeoDataFrame` """
        responses = self.get_responses_for_docs(docs)
        return self.get_geodataframe_for_responses(responses)


    @abstractmethod
    def get_payload_for_batch(self, batch:DataFrame) -> dict:
        """ for a `batch` of documents, get a dict `payload` to POST to to the target API """
        pass


    @abstractmethod
    def get_geodataframe_for_responses(self, responses:list[Response]) -> GeoDataFrame:
        """ for a list of `Response` objects, get a GeoDataFrame """
        pass


    def transform(self, subject: DataFrame) -> GeoDataFrame | None:
        return self.get_geodataframe_for_docs(subject)