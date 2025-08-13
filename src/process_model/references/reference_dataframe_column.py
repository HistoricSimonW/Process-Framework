from .reference import Reference
from pandas import Series, DataFrame
from typing import Callable, Any

class ColumnReference(Reference[Series]):
    """ a reference to a column in the value of a Reference[DataFrame] """
    def __init__(self, name:str, df:Reference[DataFrame]):
        self.name = name
        self._type = Series
        self.df = df


    def set(self, value: Series | None):
        assert self.df.has_value(), "referenced dataframe has no value; we can't assign a value to it"
        df = self.df.value
        df[self.name] = df.index.map(value)
        super().set(value)


    def __repr__(self):
        return f"ColumnReference[{'DataFrame' if self.df.has_value() else 'None'}.{self.name}]({self.value!r})"
    

