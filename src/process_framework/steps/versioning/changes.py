from dataclasses import dataclass
from pandas import Index, MultiIndex
from process_framework import AssigningStep
from process_framework.references.reference import Reference


class DetectAdditions(AssigningStep[Index]):
    def __init__(self, local:Reference[Index], remote:Reference[Index], assign_to: Reference[Index], *, overwrite: bool = True):
        super().__init__(assign_to, overwrite=overwrite)
        self.local = local
        self.remote = remote


    def generate(self) -> Index:
        local = self.local.get_value()
        remote = self.remote.get_value()
        
        # _id in local, not in remote
        return local.get_level_values(0).difference(remote.get_level_values(0))
    
    
class DetectUpdates(AssigningStep[Index]):
    def __init__(self, local:Reference[Index], remote:Reference[Index], assign_to: Reference[Index], *, overwrite: bool = True):
        super().__init__(assign_to, overwrite=overwrite)
        self.local = local
        self.remote = remote


    def generate(self) -> Index:
        local = self.local.get_value()
        remote = self.remote.get_value()
        
        # _id in local, not in remote
        return local.difference(remote).get_level_values(0)
    

class DetectDeletions(AssigningStep[Index]):
    def __init__(self, local:Reference[Index], remote:Reference[Index], assign_to: Reference[Index], *, overwrite: bool = True):
        super().__init__(assign_to, overwrite=overwrite)
        self.local = local
        self.remote = remote


    def generate(self) -> Index:
        local = self.local.get_value()
        remote = self.remote.get_value()
        
        # _id in local, not in remote
        return remote.get_level_values(0).difference(local.get_level_values(0))