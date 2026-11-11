import json
from collections.abc import Callable
from dataclasses import dataclass

from geopandas import GeoSeries
from pandas import Series
from shapely import to_geojson, to_wkt

from process_framework import TransformingStep


BNG = 27700
WGS84 = 4326

AS_WKT = (to_wkt,)
AS_GEOJSON = (to_geojson, json.loads)


@dataclass(kw_only=True)
class GetGeoPoint(TransformingStep[GeoSeries, Series]):
    """Calculate projected centroids and transform them into the required output representation."""
    working_crs: int | str = BNG
    out_crs: int | str = WGS84
    output_transformers: tuple[Callable, ...] = AS_GEOJSON

    def transform_value(self, input_: GeoSeries) -> Series:
        """Calculate centroids and apply output transformations."""
        geom = input_.to_crs(self.working_crs).centroid.to_crs(self.out_crs)

        for func in self.output_transformers:
            geom = geom.map(func)

        return geom


@dataclass(kw_only=True)
class GetGeoShape(TransformingStep[GeoSeries, Series]):
    """Reproject geometries and transform them into the required output representation."""
    out_crs: int | str = WGS84
    output_transformers: tuple[Callable, ...] = AS_GEOJSON

    def transform_value(self, input_: GeoSeries) -> Series:
        """Reproject geometries and apply output transformations."""
        geom = input_.to_crs(self.out_crs)

        for func in self.output_transformers:
            geom = geom.map(func)

        return geom