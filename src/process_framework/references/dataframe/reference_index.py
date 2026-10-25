from dataclasses import dataclass
from pandas import DataFrame, Series
from ... import ValueOrReference, resolve
from ..reference import IGettable


@dataclass
class IndexReference(IGettable[Series]):
    """Reference a DataFrame index or one of its levels."""
    df: ValueOrReference[DataFrame]
    level: int | None = None

    def has_value(self) -> bool:
        df = resolve(self.df)

        if df is None:
            return False

        if self.level is not None:
            df.index.get_level_values(self.level)

        return True


    def get_value(self) -> Series:
        df = resolve(self.df)

        if self.level is not None:
            return df.index.get_level_values(self.level).to_series(
                index=df.index
            )

        return df.index.to_series()


    def get_type(self) -> type[Series]:
        return Series