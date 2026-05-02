from contextlib import contextmanager
from elasticsearch import Elasticsearch

@contextmanager
def disable_refresh(es:Elasticsearch, index:str, refresh_on_exit:bool=True):
    """ disable an index's refresh while an operation is carried out, restoring it when the operation completes """
    settings = es.indices.get_settings(index=index)[index] 
    current = settings["settings"].get('index.refresh_interval', '1s')
    try:
        print('disabling refresh')
        es.indices.put_settings(index=index, settings={'index.refresh_interval':'-1'})
        yield
    finally:
        print('restoring refresh')
        es.indices.put_settings(index=index, settings={'index.refresh_interval':current})
        if refresh_on_exit:
            es.indices.refresh(index=index)


@contextmanager
def disable_replicas(es:Elasticsearch, index:str, refresh_on_exit:bool=True):
    """ disable an index's replica(s) while an operation is carried out, restoring it when the operation completes """
    settings = es.indices.get_settings(index=index)[index] 
    current = settings["settings"].get('index.number_of_replicas', '1')
    try:
        print('disabling replicas')
        es.indices.put_settings(index=index, settings={'index.number_of_replicas':'0'})
        yield
    finally:
        print('restoring replicas')
        es.indices.put_settings(index=index, settings={'index.number_of_replicas':current})
        if refresh_on_exit:
            es.indices.refresh(index=index)
