from .step import Step
from ..references.reference import Reference
from abc import abstractmethod, ABC

class TransformingStep[T1, T2](Step, ABC):
    """ a step that performs an type-transforming transformation to a subject, assigning the result to a different reference. The transformation can change the type of the subject """
    def __init__(self, subject:Reference[T1], assign_to:Reference[T2], *, overwrite:bool=False):
        self.subject_reference = subject
        self.assign_to = assign_to
        self.overwrite = overwrite

    @abstractmethod
    def transform(self, subject:T1) -> T2|None:
        """ if this returns a value, it is assigned to subject_reference, otherwise it's assumed that the subject has been modified in-place """
        pass

    def do(self):
        if self.assign_to.has_value() and not self.overwrite:
            return self.assign_to.get_value()
        
        subject = self.subject_reference.get_value()
        
        result = self.transform(subject)
        
        if result is None:
            return
        
        if self.assign_to is not None and isinstance(result, self.assign_to._type):
            self.assign_to.set(result)
            return
        
        raise Exception("Unhandled state")