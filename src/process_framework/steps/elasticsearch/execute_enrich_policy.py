from ..step import Step
from elasticsearch.client import Elasticsearch
from logging import info

# https://elasticsearch-py.readthedocs.io/en/v8.2.2/api.html?highlight=execute#elasticsearch.client.EnrichClient.execute_policy:~:text=the%20enrich%20policy-,execute,_policy,-(*%2C%20name%3A%20str
class ExecutePolicy(Step):
    """ execute the specified enrich policy """
    def __init__(self, elasticsearch:Elasticsearch, policy:str) -> None:
        self.elasticsearch = elasticsearch
        self.policy = policy


    def do(self):
        enrich = self.elasticsearch.enrich
        assert enrich.get_policy(name=self.policy)
        enrich.execute_policy(
            name=self.policy
        )

        enrich.execute_policy(name=self.policy)
        info(f'executed enrich policy {self.policy}')

    
    def preflight(self):
        assert self.elasticsearch.info()
        assert self.elasticsearch.enrich.get_policy(name=self.policy)