from .step import Step
from ..references import Reference
from .step import Step
from ..references import Reference

class Append(Step):
    """ append the value of `subject` to `append_to`, initializing the value of `append_to` if it is `None` """
    def __init__(self, subject:Reference, append_to:Reference[list]) -> None:
        super().__init__()
        self.subject = subject
        self.append_to = append_to

    
    def do(self):
        # if subject has no value, exit early
        if not self.subject.has_value():
            return None
        
        # get the value of `subject`
        value = self.subject.get_value()

        # if append_to has no value, initialize it to an empty list
        append_to = self.append_to

        if not append_to.has_value():
            append_to.set(list())
        
        # append `value` to the value of `append_to`
        append_to.get_value().append(value)