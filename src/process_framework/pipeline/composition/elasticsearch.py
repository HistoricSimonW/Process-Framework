from pydantic import BaseModel
from elasticsearch import Elasticsearch
from dataclasses import dataclass
from .core import mask

class ElasticsearchClientArgs(BaseModel):
    """ mixin for settings that initialize an elasticsearch client """
    cloud_id:str
    api_key:str

    def get_client(self) -> Elasticsearch:
        return Elasticsearch(**self.model_dump(
                include=set(ElasticsearchClientArgs.model_fields.keys()),
            )
        )
    
    def __repr_args__(self):
        return [
            ("cloud_id", mask(self.cloud_id)),
            ("api_key", mask(self.api_key)),
        ]

    
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