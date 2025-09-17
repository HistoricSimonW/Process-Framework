from process_framework import Step, Reference
from pandas import Series
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any, Tuple
from logging import info
from typing import Iterable


# TODO: Pull this up to Framework
class NoChangesToUpdateException(BaseException):
    """ end the run early; there are no changes to update """
    ...

# TODO: Pull this up to Framework
class AssertAnyChanges(Step):
    """ assert that the subject has a non-zero length, raise a `NoChangesToUpdateException` if not """
    def __init__(self, subject:Reference[set]) -> None:
        super().__init__()
        self.subject = subject

    
    def do(self):
        value = self.subject.get_value()
        if len(value) == 0:
            raise NoChangesToUpdateException(value)