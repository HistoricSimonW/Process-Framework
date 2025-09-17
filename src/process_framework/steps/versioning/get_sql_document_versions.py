from process_framework.steps.sql import GetOrmQueryResult
from pandas import Series
from abc import ABC

class GetSqlDocumentVersions(GetOrmQueryResult[Series], ABC):
    """ get the result of a sqlalchemy ORM query, producing a series of { _id : source}, where `source` is a scalar field that indicates the state of a document """

    def transform_result(self, result) -> Series:
        result = super().transform_result(result)
        result.index = result.index.astype(str)
        result = result.astype(str)
        return result