from process_framework import Step, Reference
from pandas import Series
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any, Tuple
from logging import info
from typing import Iterable
from pandas import Index

class NoChangesToUpdateException(BaseException):
    """ end the run early; there are no changes to update """
    ...


class AssertAnyChanges(Step):
    def __init__(self, subject:Reference[Index]) -> None:
        super().__init__()
        self.subject = subject

    def do(self):
        if len(self.subject.get_value()) == 0:
            raise NoChangesToUpdateException()