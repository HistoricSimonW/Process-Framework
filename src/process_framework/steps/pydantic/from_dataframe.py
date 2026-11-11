from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Hashable

from pandas import DataFrame, Series
from pydantic import BaseModel

from process_framework import TransformingStep
from process_framework.helpers.pydantic import gen_models_from_records


def prepare_dataframe(df: DataFrame) -> DataFrame:
    """Copy a DataFrame, adding named index levels as columns."""
    df = df.copy()

    for i, name in enumerate(df.index.names):
        if name is None:
            continue

        if name in df.columns:
            raise ValueError(f"Index name conflicts with column: {name}")

        df[name] = df.index.get_level_values(i)

    return df


def gen_records(df: DataFrame) -> Iterator[dict[Hashable, Any]]:
    """Yield non-null row dictionaries from a DataFrame."""
    for _, row in df.iterrows():
        yield row.dropna().to_dict()


@dataclass(kw_only=True)
class GetModelsForDataFrame[TOut: BaseModel](TransformingStep[DataFrame, Series]):
    """Validate DataFrame rows as models, returning an indexed Series."""

    model_type: type[TOut]

    def transform_value(self, input_: DataFrame) -> Series:
        """Construct a model for each DataFrame row."""
        df = prepare_dataframe(input_)
        
        return df.apply(
            lambda row: self.model_type.model_validate(
                row.dropna().to_dict()
            ),
            axis=1,
        )


@dataclass(kw_only=True)
class GenModelsForDataFrame[TOut: BaseModel](TransformingStep[DataFrame, Iterable[TOut]]):
    """Lazily generate models from DataFrame rows."""

    model_type: type[TOut]

    def transform_value(self, input_: DataFrame) -> Iterable[TOut]:
        """Generate validated models from non-null row records."""
        df = prepare_dataframe(input_)

        return gen_models_from_records(
            type=self.model_type,
            records=gen_records(df),
        )