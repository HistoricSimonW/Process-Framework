from pandas import Series, DataFrame, StringDtype
from typing import Callable

def get_str_cols(df:DataFrame) -> list[str]:
    return [
        col for col in df.columns
        if (
            isinstance(df[col].dtype, StringDtype)
            or (
                df[col].dtype == object
                and df[col].dropna().map(
                    lambda value: isinstance(value, str)
                ).all()
            )
        )
    ]
    
def normalize_quotes(series:Series) -> Series:
    """ normalize the typographic 'smart' quotes in a series of strs into ascii simple quotes """
    return (series
        .str.normalize('NFKD')
        .str.translate({
            ord("“"): '"',
            ord("”"): '"',
            ord("‘"): "'",
            ord("’"): "'",
        }))


def strip(series:Series) -> Series:
    return series.str.strip()


def blank_to_none(series:Series) -> Series:
    return series.replace('', None)
    
    
def apply_to_str_cols(df:DataFrame, *funcs:Callable[[Series], Series]):
    str_cols = get_str_cols(df)
    
    for func in funcs:
        df[str_cols] = df[str_cols].apply(func)
        

SHARED = (normalize_quotes, strip, blank_to_none)
""" common text transformations """