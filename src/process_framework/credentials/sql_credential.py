from process_framework.credentials.pydantic_credential import ConstructingCredentialModel
from sqlalchemy import Engine, create_engine, URL
from typing import Any

class SqlCredential(ConstructingCredentialModel[Engine]):
    DRIVERNAME:str
    HOST:str
    DATABASE:str
    QUERY_DRIVER:str|None=None
    USERNAME:str|None=None
    PASSWORD:str|None=None

    def get_client(self) -> Engine:
        # construct a SqlAlchemy engine, applying some extra logic to handle the nested 'query.driver', which is required for SQL Server
        args:dict[str, Any] = {k.lower():v for k, v in self.model_dump(exclude_none=True, exclude={'QUERY_DRIVER'})}
        if self.QUERY_DRIVER:
            args['query'] = dict(driver=self.QUERY_DRIVER)
        url = URL.create(**args)
        return create_engine(url)