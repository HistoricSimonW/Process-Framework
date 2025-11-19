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
        args = {k:v for k, v in kwargs.items() if k not in {'query_driver'}}
        if 'query_driver' in kwargs:
            args['query'] = dict(driver=kwargs['query_driver'])
            if kwargs.get('query_trust_server_certificate'):
                args['query']['TrustServerCertificate'] = kwargs['query_trust_server_certificate']
        url = URL.create(**args)
        return create_engine(url)