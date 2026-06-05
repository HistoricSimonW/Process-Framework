from dataclasses import dataclass
from pandas import Index, MultiIndex
from process_framework import AssigningStep
from process_framework import Reference
from process_framework.references.composition.core import IGettable

@dataclass(kw_only=True)
class HasLocalAndRemote[T]:
    """ mixin for classes that compare local and remote instances of things """
    local:IGettable[T]
    remote:IGettable[T]

@dataclass(kw_only=True)
class DetectAdditions(HasLocalAndRemote[Index], AssigningStep[Index]):
    """ detect items in Local that are not in Remote """
    def generate_value(self) -> Index | None:
        local = self.local.get_value()
        remote = self.remote.get_value()
        
        # _id in local, not in remote
        return local.get_level_values(0).difference(remote.get_level_values(0))
    

@dataclass(kw_only=True)
class DetectUpdates(HasLocalAndRemote[Index], AssigningStep[Index]):
    """ return items in Local that are not in Remote, or with different values to Remote """
    def generate_value(self) -> Index | None:
        local = self.local.get_value()
        remote = self.remote.get_value()
        
        # _id in local, not in remote
        return local.difference(remote).get_level_values(0)
    

class DetectDeletions(HasLocalAndRemote[Index], AssigningStep[Index]):
    """ return items in Remote that are not in Local """
    def generate_value(self) -> Index | None:
        local = self.local.get_value()
        remote = self.remote.get_value()
                
        # _id in local, not in remote
        return remote.get_level_values(0).difference(local.get_level_values(0))