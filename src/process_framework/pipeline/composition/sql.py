from pydantic import BaseModel, Field
from sqlalchemy import URL, Engine, create_engine
from process_framework.pipeline.composition.masking import HasMaskedFields, Masked
from typing import Annotated

class EngineQueryArgs(BaseModel):
    """ nested model for arguments passed into `URL.create(query={x})` """
    driver:str|None
    trust_server_certificate:str|None = Field(None,serialization_alias='TrustServerCertificate')
    

class EngineArgs(HasMaskedFields, BaseModel):
    """ mixin for settings that describe a database engine """
    host:str
    database:str
    username:Annotated[str|None, Masked(2)]=None
    password:Annotated[str|None, Masked(2)]=None
    drivername:str
    query:EngineQueryArgs


    def get_url(self) -> URL:
        return URL.create(
            **self.model_dump(
                include=set(EngineArgs.model_fields.keys()),
                by_alias=True, 
                exclude_none=True
            )
        )

    def get_engine(self) -> Engine:
        url = self.get_url()
        return create_engine(url)
    
if __name__ == '__main__':
    EngineArgs(
        host='host',
        database='database',
        drivername='drivername',
        query=EngineQueryArgs(driver=None, trust_server_certificate=None)
    )