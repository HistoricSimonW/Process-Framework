from typing import Sequence
from elasticsearch import Elasticsearch
from process_framework.credentials.pydantic_credential import ConstructingCredentialModel

class ElasticSearchCredential(ConstructingCredentialModel[Elasticsearch]):
    """ a `ConstructingCredentialModel` for the Elasticsearch client """
    CLOUD_ID:str
    USER:str|None=None
    PASSWORD:str|None=None
    API_KEY:str|Sequence[str]|None=None


    @classmethod
    def __client_from_kwargs__(cls, **kwargs) -> Elasticsearch:
        return Elasticsearch(**kwargs)