from pandas import DataFrame, Series
from geopandas import GeoDataFrame, GeoSeries

from process_framework import Reference, TransformingStep

class TransformGeometryToWkt[T:(GeoSeries, GeoDataFrame)](TransformingStep[T, Series]):
    """ transform a n input GeoSeries or GeoDataFrame to a Series of WKT strings """
    def __init__(self, subject: Reference[T], assign_to: Reference[Series], 
                 out_crs:int=4326, buffer:float=0,
                 rounding_precision:int=5, trim:bool=True, *, overwrite:bool=True):
        super().__init__(subject, assign_to, overwrite=overwrite)
        self.out_crs=out_crs
        self.rounding_precision=rounding_precision
        self.trim =trim
        self.buffer=buffer


    def transform(self, subject: T) -> Series | None:

        if isinstance(subject, GeoDataFrame):
            geometry = subject.geometry
        elif isinstance(subject, GeoSeries):
            geometry = subject
        
        if self.buffer:
            geometry = geometry.to_crs(27700).buffer(self.buffer).to_crs(self.out_crs)
            
        assert isinstance(geometry, GeoSeries)

        return geometry.buffer(self.buffer).to_crs(self.out_crs).to_wkt(
            rounding_precision=self.rounding_precision,
            trim=self.trim
        )

