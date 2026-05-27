from pydantic import BaseModel
from elasticsearch import Elasticsearch
from dataclasses import dataclass

class ElasticsearchClientArgs(BaseModel):
    """ mixin for settings that initialize an elasticsearch client """
    cloud_id:str
    api_key:str

    def get_client(self) -> Elasticsearch:
        return Elasticsearch(**self.model_dump(
                include=set(ElasticsearchClientArgs.model_fields.keys()),
            )
        )

    
class ElasticsearchIndexArgs(BaseModel):
    """ mixin for settings that have an elasticsearch index """
    index:str


class ElasticsearchIngestPipelineArgs(BaseModel):
    """ mixin for settings that have an elasticsearch ingest pipeline """
    ingest_pipeline:str


@dataclass
class HasElasticsearchClient:
    """ mixin for clients with an elasticsearch instance """
    elasticsearch:Elasticsearch