from process_framework import Step, Reference
from process_framework.exceptions import NoChangesToUpdate
from pandas import Index


class AssertAnyChanges(Step):
    def __init__(self, subject:Reference[Index]) -> None:
        super().__init__()
        self.subject = subject


    def do(self):
        if len(self.subject.get_value()) == 0:
            raise NoChangesToUpdate()