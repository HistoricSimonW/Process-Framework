
from collections.abc import Iterator

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from process_framework.steps.elasticsearch.transformation import (
    as_dataframe,
    as_first,
    as_value_set,
    dataframe_from_hits,
    first_as_type,
    flatten_hit,
    get_first,
    get_id,
    get_ids_set,
    get_series,
    get_value_series,
    get_value_set,
)


@pytest.fixture
def hits() -> list[dict]:
    """Return representative Elasticsearch hits."""
    return [
        {
            "_id": "1",
            "_index": "test-index",
            "_source": {
                "title": "First",
                "status": "published",
                "count": 10,
            },
            "fields": {
                "tags": ["a", "b"],
                "year": [2020],
            },
        },
        {
            "_id": "2",
            "_index": "test-index",
            "_source": {
                "title": "Second",
                "status": "draft",
                "count": 20,
            },
            "fields": {
                "tags": ["b", "c"],
                "year": [2021],
            },
        },
    ]


def test_get_id(hits):
    assert get_id(hits[0]) == "1"


def test_get_ids_set(hits):
    assert get_ids_set(hits) == {"1", "2"}


def test_get_ids_set_deduplicates(hits):
    assert get_ids_set([*hits, hits[0]]) == {"1", "2"}


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([1, 2, 3], 1),
        (["a", "b"], "a"),
        ((None, 2), None),
    ],
)
def test_get_first(values, expected):
    assert get_first(values) == expected


def test_get_first_empty():
    with pytest.raises(IndexError):
        get_first([])


@pytest.mark.parametrize(
    ("values", "as_type", "expected"),
    [
        (["123"], int, 123),
        (["1.5"], float, 1.5),
        ([123], str, "123"),
    ],
)
def test_first_as_type(values, as_type, expected):
    assert first_as_type(values, as_type) == expected


def test_as_first():
    transform = as_first(int)

    assert transform(["123", "456"]) == 123


def test_get_value_set(hits):
    assert get_value_set(hits, "status") == {"published", "draft"}


def test_get_value_set_with_transformer(hits):
    result = get_value_set(
        hits,
        field="year",
        source_or_fields="fields",
        value_transformer=get_first,
    )

    assert result == {2020, 2021}


def test_as_value_set(hits):
    transform = as_value_set(
        field="year",
        source_or_fields="fields",
        value_transformer=as_first(str),
    )

    assert transform(hits) == {"2020", "2021"}


def test_get_series(hits):
    result = get_series(
        hits,
        value_func=lambda hit: hit["_source"]["count"],
    )

    expected = pd.Series({"1": 10, "2": 20})

    assert_series_equal(result, expected)


def test_get_series_custom_id(hits):
    result = get_series(
        hits,
        value_func=lambda hit: hit["_source"]["title"],
        id_func=lambda hit: f"doc-{hit['_id']}",
    )

    expected = pd.Series({"doc-1": "First", "doc-2": "Second"})

    assert_series_equal(result, expected)


def test_get_value_series(hits):
    result = get_value_series(hits, field="count")

    expected = pd.Series({"1": 10, "2": 20})

    assert_series_equal(result, expected)


def test_get_value_series_with_transformer(hits):
    result = get_value_series(
        hits,
        field="year",
        source_or_fields="fields",
        value_transformer=get_first,
    )

    expected = pd.Series({"1": 2020, "2": 2021})

    assert_series_equal(result, expected)


def test_flatten_hit(hits):
    result = flatten_hit(hits[0])

    assert result == {
        "_id": "1",
        "_index": "test-index",
        "title": "First",
        "status": "published",
        "count": 10,
        "tags": ["a", "b"],
        "year": [2020],
    }


def test_flatten_hit_precedence():
    hit = {
        "_id": "metadata",
        "_source": {"_id": "source", "value": 1},
        "fields": {"value": 2},
    }

    result = flatten_hit(hit)

    assert result["_id"] == "metadata"
    assert result["value"] == 2


def test_flatten_hit_does_not_mutate():
    hit = {
        "_id": "1",
        "_source": {"value": 1},
        "fields": {"value": 2},
    }

    original = {
        "_id": "1",
        "_source": {"value": 1},
        "fields": {"value": 2},
    }

    flatten_hit(hit)

    assert hit == original


@pytest.mark.parametrize(
    "hit",
    [
        {"_id": "1"},
        {"_id": "1", "_source": None},
        {"_id": "1", "fields": None},
    ],
)
def test_flatten_hit_missing_sections(hit):
    assert flatten_hit(hit) == {"_id": "1"}


def test_dataframe_from_hits(hits):
    result = dataframe_from_hits(hits)

    assert result.index.tolist() == ["1", "2"]
    assert result["title"].tolist() == ["First", "Second"]
    assert result["count"].tolist() == [10, 20]
    assert result["year"].tolist() == [[2020], [2021]]


def test_dataframe_from_hits_no_index(hits):
    result = dataframe_from_hits(hits, index=None)

    assert isinstance(result.index, pd.RangeIndex)
    assert result["_id"].tolist() == ["1", "2"]


def test_dataframe_from_hits_assign(hits):
    result = dataframe_from_hits(
        hits,
        year=lambda df: df["year"].str[0],
        decade=lambda df: df["year"] // 10 * 10,
    )

    assert result["year"].tolist() == [2020, 2021]
    assert result["decade"].tolist() == [2020, 2020]


def test_dataframe_from_hits_custom_index(hits):
    result = dataframe_from_hits(
        hits,
        index="title",
    )

    assert result.index.tolist() == ["First", "Second"]


def test_dataframe_from_hits_dtype(hits):
    result = dataframe_from_hits(
        hits,
        dtype={"count": "float64"},
    )

    assert result["count"].dtype == "float64"


def test_dataframe_from_hits_drop_cols(hits):
    result = dataframe_from_hits(
        hits,
        drop_cols=["_index", "status"],
    )

    assert "_index" not in result.columns
    assert "status" not in result.columns


@pytest.mark.parametrize(
    "renames",
    [
        {"title": "name"},
        lambda col: "name" if col == "title" else col,
    ],
)
def test_dataframe_from_hits_renames(hits, renames):
    result = dataframe_from_hits(hits, renames=renames)

    assert "name" in result.columns
    assert "title" not in result.columns


def test_dataframe_from_hits_fillna(hits):
    hits[0]["_source"]["description"] = None
    hits[1]["_source"]["description"] = "Present"

    result = dataframe_from_hits(
        hits,
        fillna={"description": "Missing"},
    )

    assert result["description"].tolist() == ["Missing", "Present"]


def test_dataframe_from_hits_explode(hits):
    result = dataframe_from_hits(
        hits,
        explode="tags",
    )

    assert result["tags"].tolist() == ["a", "b", "b", "c"]
    assert result.index.tolist() == ["1", "1", "2", "2"]


def test_dataframe_from_hits_generator(hits):
    def generate() -> Iterator[dict]:
        yield from hits

    result = dataframe_from_hits(generate())

    assert len(result) == 2


def test_as_dataframe(hits):
    transform = as_dataframe(
        index="_id",
        drop_cols=["_index"],
        renames={"title": "name"},
        count_doubled=lambda df: df["count"] * 2,
    )

    result = transform(hits)

    expected = dataframe_from_hits(
        hits,
        index="_id",
        drop_cols=["_index"],
        renames={"title": "name"},
        count_doubled=lambda df: df["count"] * 2,
    )

    assert_frame_equal(result, expected)
