from collections.abc import Iterable
from dataclasses import dataclass

from pandas import DataFrame, Series
from pydantic import BaseModel

from process_framework import TransformingStep
from process_framework.helpers.pydantic import gen_models_from_records


def prepare_dataframe(df: DataFrame) -> DataFrame:
    """Copy a DataFrame, adding named index levels as columns."""
    df = df.copy()

    for i, name in enumerate(df.index.names):
        if name is not None:
            df[name] = df.index.get_level_values(i)

    return df


@dataclass
class GetModelsForDataFrame[T: BaseModel](TransformingStep[DataFrame, Series]):
    """Validate DataFrame rows as Pydantic models, returning a Series."""

    model_type: type[T]

    def transform_value(self, input_: DataFrame) -> Series:
        """Convert each row into a validated model."""
        df = prepare_dataframe(input_)
        records = df.apply(lambda row: row.dropna().to_dict(), axis=1)
        return records.map(self.model_type.model_validate)


@dataclass
class GenModelsForDataFrame[T: BaseModel](TransformingStep[DataFrame, Iterable[T]]):
    """Generate Pydantic models lazily from DataFrame rows."""

    model_type: type[T]

    def transform_value(self, input_: DataFrame) -> Iterable[T]:
        """Generate validated models from non-null row values."""
        df = prepare_dataframe(input_)

        def gen_records() -> Iterable[dict]:
            for _, row in df.iterrows():
                yield row.dropna().to_dict()

        return gen_models_from_records(
            type=self.model_type,
            records=gen_records(),
        )