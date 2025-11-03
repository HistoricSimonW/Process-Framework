from ..reference import Reference
from pandas import Series, DataFrame
from typing import Iterable


class ColumnReference(Reference[Series]):
    """ a reference to a column in the value of a Reference[DataFrame] """
    def __init__(self, df:Reference[DataFrame], column:str, column_as_index:"str|ColumnReference|None"=None):
        self._type = Series
        self.df = df
        self.column = column
        self.column_as_index = (
            column_as_index.column if isinstance(column_as_index, ColumnReference) 
            else column_as_index
        )


    def is_instance_of(self, class_or_tuple) -> bool:
        if not self.has_value():
            return False
        
        if isinstance(class_or_tuple, type):
            return class_or_tuple == Series
        
        if isinstance(class_or_tuple, Iterable):
            return Series in class_or_tuple
        
        return False
    
    
    def has_value(self) -> bool:
        return self.df.has_value() and (self.column in self.df.get_value().columns)
    

    def set(self, value: Series | None):
        if value is None:
            self.value = None
            return
        
        assert isinstance(value, Series), f'expected value of type `Series`, got {Series}'
        
        df = self.df.value
        assert isinstance(df, DataFrame), "referenced dataframe has not been assigned; we can't assign a value to a column in it"
        
        # we can assign based on index (default) or use a `column_as_index` and map to that instead
        if isinstance(self.column_as_index, str):
            df[self.column] = df[self.column_as_index].map(value)
        else:
            df[self.column] = df.index.map(value)


    def get_value(self) -> Series:
        """ return the column `self.column` of `self.df`; throws an error if `self.df` is not set, or `self.column` is not in the `DataFrame` """
        return self.df.get_value()[self.column]