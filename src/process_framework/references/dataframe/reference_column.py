from ..reference import Reference
from pandas import Series, DataFrame
from typing import Iterable, cast


class ColumnReference[T:Series](Reference[T]):
    """ a reference to a column in the value of a Reference[DataFrame] """
    def __init__(self, df:Reference[DataFrame], column:str, column_as_index:"str|ColumnReference|None"=None, type:type[T]=Series):
        super().__init__(type, None)
        self.df = df
        self.column = column
        self.column_as_index = (
            column_as_index.column if isinstance(column_as_index, ColumnReference) 
            else column_as_index
        )
    
    
    def has_value(self) -> bool:
        return self.df.has_value() and (self.column in self.df.get_value().columns)
    
    def set_value(self, value: T | None) -> None:
        if value is None:
            self.value = None
            return
        
        assert isinstance(value, self._type), f'expected value of type `Series`, got {Series}'
        
        try:
            df = self.df.get_value()
        except Exception as e:
            raise ValueError("referenced dataframe has not been assigned; we can't assign a value to a column in it") from e
        
        # we can assign based on index (default) or use a `column_as_index` and map to that instead
        if isinstance(self.column_as_index, str):
            df[self.column] = df[self.column_as_index].map(value)
        else:
            df[self.column] = df.index.map(value) # type:ignore
    

    def get_value(self) -> T:
        v = self.df.get_value()[self.column]
        return cast(T, v)