from typing import Iterable, Sequence
from pydantic_geojson import FeatureModel, FeatureCollectionModel, MultiPointModel, PointModel
from geopandas import GeoDataFrame, GeoSeries
from pandas import Series
import json


"""Utilities for adapting pydantic_geojson models to GeoPandas."""

def strip_z(coords:Sequence[float]|Sequence[int]|Sequence[Sequence]):
    """Recursively remove trailing null Z values from GeoJSON coordinates."""
    if isinstance(coords[0], (float, int)):
        # coordinate tuple
        return coords[:2]

    return [strip_z(c) for c in coords] # type:ignore


def clean_geometry(geometry_dict):
    """Return a geometry dict with null Z coordinates removed."""
    geometry = geometry_dict.copy()
    try:
        geometry["coordinates"] = strip_z(geometry["coordinates"])
    except Exception as e:
        print(geometry)
        raise e
    return geometry


def clean_model(model:dict) -> dict:
    """Return a feature model with cleaned geometry coordinates."""
    model = model.copy()
    model['geometry'] = clean_geometry(model['geometry'])
    return model


def geodataframe_from_pydantic_features(features:Iterable[FeatureModel], **from_features_kwargs) -> GeoDataFrame:
    """Create a GeoDataFrame from pydantic GeoJSON features."""
    assert 'crs' not in from_features_kwargs
    return GeoDataFrame.from_features(
        [clean_model(f.model_dump()) for f in features],
        crs=4326,
        **from_features_kwargs
    )


def gen_features_from_collections(collections:Iterable[FeatureCollectionModel]) -> Iterable[FeatureModel]:
    """Yield features from GeoJSON feature collections."""
    for collection in collections:
        yield from collection.features


def geodataframe_from_pydantic_feature_collections(collections:Iterable[FeatureCollectionModel], coerce_multipoints_to_points:bool=False, dissolve_by:str|None=None, **from_features_kwargs) -> GeoDataFrame:
    """Create a GeoDataFrame from pydantic GeoJSON collections."""
    features = list(gen_features_from_collections(collections))
    
    if len(features) == 0:
        return GeoDataFrame()

    gdf = geodataframe_from_pydantic_features(
        features, **from_features_kwargs
    )

    if dissolve_by is not None:
        gdf = gdf.dissolve(by=dissolve_by, as_index=False)

    if coerce_multipoints_to_points:
        gdf.geometry = gdf.geometry.explode(ignore_index=False).groupby(level=0).agg('first')

    return gdf


def get_json_geometry_for_feature(feature:dict) -> dict:
    """Validate a feature and return its GeoJSON geometry."""
    m = feature
    return clean_geometry(m['geometry'])
    

def geoseries_to_geojson_series(s:GeoSeries) -> Series:
    """Convert a GeoSeries to a Series of GeoJSON geometries."""
    index_type = s.index.dtype
    jsons = s.geometry.to_json(to_wgs84=True)
    data = json.loads(jsons)
    features = {feature.pop('id'):get_json_geometry_for_feature(feature)
                for feature in data['features']}
    result = Series(features)
    result.index = result.index.astype(index_type)
    return result