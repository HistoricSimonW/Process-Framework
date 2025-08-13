from .step import Step
from ..references.reference import Reference
from abc import abstractmethod, ABC

class ModifyingStep[T](Step, ABC):
    """ a step that modifies an input value without changing its type, optionally assigning the result to a different reference. the subject and assignee references are of the same type """
    def __init__(self, subject:Reference[T], assign_to:Reference[T]|None=None):
        self.subject_reference = subject
        self.assign_to = assign_to

    @abstractmethod
    def transform(self, subject:T) -> T|None:
        """ if this returns a value, it is assigned to subject_reference, otherwise it's assumed that the subject has been modified in-place """
        pass

    def do(self):
        subject = self.subject_reference.get_value()
        
        if subject is None:
            return
        
        result = self.transform(subject)
        # print('result', result)
        
        if result is None:
            return
        
        if self.assign_to is not None and isinstance(result, self.assign_to._type):
            # print(('to-other', self.assign_to), ('current', self.assign_to.value), ('new', result))
            self.assign_to.set(result)
            # print(('post-set', self.assign_to.value))
            return
        
        if isinstance(result, self.subject_reference._type):
            # print(('in-place', self.assign_to), ('current', self.subject_reference.value), ('new', result))
            self.subject_reference.set(result)
            # print(('post-set', self.subject_reference.value))
            return
        
        raise Exception("Unhandled state")
    

class TransformingStep[T1, T2](Step, ABC):
    """ a step that performs an type-transforming transformation to a subject, assigning the result to a different reference. The transformation can change the type of the subject """
    def __init__(self, subject:Reference[T1], assign_to:Reference[T2]):
        self.subject_reference = subject
        self.assign_to = assign_to

    @abstractmethod
    def transform(self, subject:T1) -> T2|None:
        """ if this returns a value, it is assigned to subject_reference, otherwise it's assumed that the subject has been modified in-place """
        pass

    def do(self):
        subject = self.subject_reference.get_value()
        
        if subject is None:
            return
        
        result = self.transform(subject)
        # print('result', result)
        
        if result is None:
            return
        
        if self.assign_to is not None and isinstance(result, self.assign_to._type):
            # print(('to-other', self.assign_to), ('current', self.assign_to.value), ('new', result))
            self.assign_to.set(result)
            # print(('post-set', self.assign_to.value))
            return
        
        raise Exception("Unhandled state")