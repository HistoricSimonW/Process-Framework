from pydantic import BaseModel
from elasticsearch import Elasticsearch

class ElasticsearchClientArgs(BaseModel):
    cloud_id:str
    api_key:str

    def get_client(self) -> Elasticsearch:
        return Elasticsearch(**self.model_dump(
            include=set(ElasticsearchClientArgs.model_fields.keys()),
        ))

    
class ElasticsearchIndexArgs(BaseModel):
    index:str


class ElasticsearchIngestPipelineArgs(BaseModel):
    ingest_pipeline:str