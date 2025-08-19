from .reference import Reference
from pandas import Series, DataFrame
from typing import Any

class ColumnReference(Reference[Series]):
    """ a reference to a column in the value of a Reference[DataFrame] """
    def __init__(self, df:Reference[DataFrame], column:str):
        self._type = Series
        self.df = df
        self.column = column


    def set(self, value: Series | None):
        assert self.df.has_value(), "referenced dataframe has no value; we can't assign a value to it"
        df = self.df.value
        assert isinstance(df, DataFrame)
        df[self.column] = df.index.map(value)
        super().set(value)


    def get_value(self, throw_on_none: bool = False) -> Series | None:
        assert self.df.has_value(), "referenced dataframe has no value; we can't get a value from it"
        df = self.df.value
        assert isinstance(df, DataFrame)
        return df[self.column]
    

    def __repr__(self):
        return f"ColumnReference[{'DataFrame' if self.df.has_value() else 'None'}.{self.column}]({self.value!r})"
    

