from pydantic import BaseModel, Field
from sqlalchemy import URL, Engine, create_engine

class EngineQueryArgs(BaseModel):
    """ nested model for arguments passed into `URL.create(query={x})` """
    driver:str|None
    trust_server_certificate:str|None = Field(None,serialization_alias='TrustServerCertificate')
    

class DatabaseArgs(BaseModel):
    """ mixin for settings that describe a connection to a database """
    host:str
    database:str
    username:str
    password:str
    drivername:str
    query:EngineQueryArgs

    def get_url(self) -> URL:
        return URL.create(
            **self.model_dump(
                include=set(DatabaseArgs.model_fields.keys()),
                by_alias=True, 
                exclude_none=True
            )
        )

    def get_engine(self) -> Engine:
        url = self.get_url()
        return create_engine(url)