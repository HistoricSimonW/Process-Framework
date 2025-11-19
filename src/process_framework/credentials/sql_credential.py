from process_framework.credentials.pydantic_credential import ConstructingCredentialModel
from sqlalchemy import Engine, create_engine, URL
from typing import Any

class SqlCredential(ConstructingCredentialModel[Engine]):
    DRIVERNAME:str
    HOST:str
    DATABASE:str
    QUERY_DRIVER:str|None=None
    QUERY_TRUST_SERVER_CERTIFICATE:str|None=None
    USERNAME:str|None=None
    PASSWORD:str|None=None
   

    @classmethod
    def __client_from_kwargs__(cls, **kwargs) -> Engine:
        query_args = {'query_driver', 'query_trust_server_certificate'}
        args = {k:v for k, v in kwargs.items() if k not in query_args}

        if any(q in kwargs for q in query_args):
            args['query'] = dict()

        if 'query_driver' in kwargs:
            args['query']['driver']=kwargs['query_driver']
        
        if 'query_trust_server_certificate' in kwargs:
            args['query']['TrustServerCertificate'] = kwargs['query_trust_server_certificate']
        url = URL.create(**args)
        return create_engine(url)