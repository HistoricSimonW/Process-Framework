from pydantic import BaseModel
from elasticsearch import Elasticsearch

class ElasticsearchClientArgs(BaseModel):
    cloud_id:str
    api_key:str

    def get_client(self) -> Elasticsearch:
        return Elasticsearch(**self.model_dump())

    
class ElasticsearchIndexArgs(BaseModel):
    index:str


class ElasticsearchIngestPipelineArgs(BaseModel):
    ingest_pipeline:str