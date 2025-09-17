from process_framework import Reference
from pandas import Series
from process_framework import AssigningStep
import logging
from typing import Literal
from process_framework.references import _repr

LOCAL_FROM_REMOTE = 'local_from_remote'
REMOTE_FROM_LOCAL = 'remote_from_local' 
SYMMETRIC = 'symmetric'
    
class GetIndexChanges(AssigningStep[list]):
    """ compare the indices of two series; get a list of the differences between the indices; values are ignored
        see also `GetVersionChanges`, which compares values """

    def __init__(self, assign_to: Reference[list], local:Reference[Series], remote:Reference[Series], 
                 how:Literal['local_from_remote', 'remote_from_local', 'symmetric'], *, overwrite:bool=True):
        super().__init__(assign_to)
        self.local = local
        self.remote = remote
        self.how = how
        self.overwrite = overwrite

        
    def generate(self) -> list:
        
        logging.info('local', len(self.local.get_value()), 'remote', len(self.remote.get_value()))
        local = self.local.get_value()
        remote = self.remote.get_value()
        assert isinstance(local, Series) and isinstance(remote, Series)
        
        # a set to collect unique ids of changes
        changes = set()

        # get the symemtric difference of the indices ('a not in b' or 'b not in a')
        if self.how == LOCAL_FROM_REMOTE:
            changes.update(local.index.difference(remote.index))
        elif self.how == REMOTE_FROM_LOCAL: 
            changes.update(remote.index.difference(local.index))
        elif self.how == SYMMETRIC:
            changes.update(local.index.symmetric_difference(remote.index)) # type: ignore
        else:
            raise NotImplementedError()
        
        # index_symmetric_diff = local.index.symmetric_difference(remote.index) # type: ignore
        logging.info(('got difference', self.how, len(changes)))
        logging.info(('changes:', _repr.repr(changes)))

        return list(changes)


class GetVersionChanges(AssigningStep[list]):
    """ compare the indices of two series; get a list of the differences between the indices; values are ignored
        see also `GetVersionChanges`, which compares values """
    
    def __init__(self, assign_to: Reference[list], local:Reference[Series], remote:Reference[Series], how:Literal['local_from_remote', 'remote_from_local', 'symmetric']):
        super().__init__(assign_to)
        self.local = local
        self.remote = remote
        self.how = how


    def generate(self) -> list:
        logging.info('local', len(self.local.get_value()), 'remote', len(self.remote.get_value()))
        local = self.local.get_value()
        remote = self.remote.get_value()
        assert isinstance(local, Series) and isinstance(remote, Series)
        
        # a set to collect unique ids of changes
        changes = set()

        # get the symemtric difference of the indices ('a not in b' or 'b not in a')
        if self.how == LOCAL_FROM_REMOTE:
            changes.update(local.index.difference(remote.index))
        elif self.how == REMOTE_FROM_LOCAL: 
            changes.update(remote.index.difference(local.index))
        elif self.how == SYMMETRIC:
            changes.update(local.index.symmetric_difference(remote.index)) # type: ignore
        else:
            raise NotImplementedError()
        
        # index_symmetric_diff = local.index.symmetric_difference(remote.index) # type: ignore
        logging.info(('got difference', self.how, len(changes)))
        logging.info(('changes:', _repr.repr(changes)))

        # get differences of versions (where local version != remote version)
        versions_diff = local[(local != local.index.map(remote))]
        print(('versions diff', len(versions_diff)))
        changes.update(versions_diff.index)

        print(('total diff', len(changes)))
        logging.info(f'{type(self).__name__}, got ids with changes {changes}')
        return list(changes)
