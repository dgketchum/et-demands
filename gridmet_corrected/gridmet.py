import os
from datetime import datetime

import ee
import rasterio
from shapely.geometry.point import Point
import geopandas as gpd

from landsat.ee_utils import is_authorized
from gridmet_corrected.thredds import GridMet


def export_openet_correction_surfaces():
    is_authorized()

    for etref in ['etr', 'eto']:
        id_ = 'projects/openet/reference_et/gridmet/ratios/v1/monthly/{}'.format(etref)
        c = ee.ImageCollection(id_)
        scenes = c.aggregate_histogram('system:index').getInfo()
        for k in list(scenes.keys()):
            month_number = datetime.strptime(k, '%b').month
            desc = 'gridmet_corrected_{}_{}'.format(etref, month_number)
            i = ee.Image(os.path.join(id_, k))
            task = ee.batch.Export.image.toCloudStorage(
                i,
                description=desc,
                bucket='wudr',
                fileNamePrefix=desc,
                crs='EPSG:4326')
            task.start()
            print(desc)


def gridmet_centroids(tif, fields, gridmet_points):

    shapefile = gpd.read_file(fields)
    bounds = shapefile.total_bounds

    with rasterio.open(tif, masked=True, bounds=bounds) as src:
        transform = src.transform
        cell_size_x = transform.a
        cell_size_y = -transform.e
        rows, cols = src.shape
        centroids = []

        for row in range(rows):
            for col in range(cols):
                x = transform.c + col * cell_size_x + cell_size_x / 2
                y = transform.f + row * cell_size_y + cell_size_y / 2
                centroids.append((x, y))

    idx = [i for i in range(1, len(centroids) + 1)]
    df = gpd.GeoDataFrame(index=idx, columns=['GFID', 'geometry'])
    df['geometry'] = [Point(c) for c in centroids]
    df.to_file(gridmet_points, crs='EPSG:4326')


def corrected_gridmet(points_shp, out_dir_, start='1987-01-01', end='2021-12-31', variable='eto'):
    df = gpd.read_file(points_shp)

    for i, r in df.iterrows():
        point = r['geometry']
        lat, lon = point.y, point.x
        g = GridMet('eto', start='1987-01-01', end='2021-12-31', lat=lat, lon=lon)
        df = g.get_point_timeseries()
        df.to_csv(os.path.join(out_dir_, '{}_{}.csv'.format(r['GFID'], variable)))


if __name__ == '__main__':
    d = '/home/dgketchum/PycharmProjects/et-demands/'

    export_openet_correction_surfaces()

    raster = os.path.join(d, 'gridmet_corrected', 'correction_surfaces', '')
    fields_shp = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample.shp')
    grimet_cent = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_gridmet_centroids.shp')
    # gridmet_centroids(raster, fields_shp, grimet_cent)

# ========================= EOF ====================================================================
