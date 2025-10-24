from ..reference import Reference
from pandas import Series, DataFrame
from typing import Iterable


class IndexReference(Reference[Series]):
    """ a reference to the index, or a level of the index, of the value of a Reference[DataFrame] """
    def __init__(self, df:Reference[DataFrame], level:int|None=None):
        self._type = Series
        self.df = df
        self.level=level
    

    def is_instance_of(self, class_or_tuple) -> bool:
        if not self.has_value():
            return False
        
        if isinstance(class_or_tuple, type):
            return class_or_tuple == Series
        
        if isinstance(class_or_tuple, Iterable):
            return Series in class_or_tuple
        
        return False


    def has_value(self) -> bool:
        """ returns True if self.df has a value; throws an error if self.df has a value, but its index lacks the specified `level` """
        try:
            df:DataFrame = self.df.get_value()
        except:
            return False
        
        if not isinstance(self.level, int):
            return True
        
        assert df.index.get_level_values(self.level)
        return True


    def get_value(self) -> Series:
        """ get the values of `self.df.index` (optionally at `level` if specified) as a Series """
        df:DataFrame = self.df.get_value()
        if isinstance(self.level, int):
            return df.index.get_level_values(self.level).to_series()
        return df.index.to_series()