import os
from tqdm import tqdm
from datetime import datetime

import ee
import geopandas as gpd
from rasterstats import zonal_stats

from landsat.ee_utils import is_authorized
from gridmet_corrected.thredds import GridMet

CRS_TRANSFORM = [0.041666666666666664,
                 0, -124.78749996666667,
                 0, -0.041666666666666664,
                 49.42083333333334]


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
                dimensions='1386x585',
                fileNamePrefix=desc,
                crsTransform=CRS_TRANSFORM,
                crs='EPSG:4326')
            task.start()
            print(desc)


def corrected_gridmet(fields, gridmet_points, fields_join, gridmet_csv_dir, gridmet_ras, variable='eto'):
    """This depends on running 'Raster Pixels to Points' on a WGS Gridmet raster,
     attributing GFID, lat, and lon in the attribute table, and saving to project crs: 5071.
     GFID is an arbitrary identifier e.g., $row_number. It further depends on projecting the
     rasters to EPSG:5071, usng the project.sh bash script"""

    print('Find field-gridmet joins')

    if variable == 'eto':
        thredds_var = 'pet'
    else:
        thredds_var = variable

    gridmet_pts = gpd.read_file(gridmet_points)
    fields = gpd.read_file(fields)

    rasters = [os.path.join(gridmet_ras, 'gridmet_corrected_{}_{}.tif'.format(variable, m)) for m in range(1, 13)]

    gridmet_targets = {}
    for i, field in tqdm(fields.iterrows(), total=fields.shape[0]):
        min_distance = 1e13
        closest_fid = None

        for j, g_point in gridmet_pts.iterrows():
            distance = field['geometry'].centroid.distance(g_point['geometry'])

            if distance < min_distance:
                min_distance = distance
                closest_fid = g_point['GFID']
                closest_geo = g_point['geometry']

        fields.at[i, 'GFID'] = closest_fid
        if closest_fid not in gridmet_targets.keys():
            gdf = gpd.GeoDataFrame({'geometry': [closest_geo]})
            first = True
            for i, r in enumerate(rasters, start=1):
                stats = zonal_stats(gdf, r, stats=['mean'])[0]['mean']
                if first:
                    gridmet_targets[closest_fid] = {i: stats}
                    first = False
                else:
                    gridmet_targets[closest_fid].update({i: stats})

    fields.to_file(fields_join, crs='EPSG:5071')

    len_ = len(gridmet_targets.keys())
    print('Get gridmet for {} target points'.format(len_))
    gridmet_pts.index = gridmet_pts['GFID']
    for k, v in tqdm(gridmet_targets.items(), total=len_):
        r = gridmet_pts.loc[k]
        lat, lon = r['lat'], r['lon']
        g = GridMet(thredds_var, start='1987-01-01', end='2021-12-31', lat=lat, lon=lon)
        df = g.get_point_timeseries()
        df.columns = [variable]

        for month in range(1, 13):
            corr_factor = v[month]
            idx = [i for i in df.index if i.month == month]
            df.loc[idx, '{}_corr'.format(variable)] = df.loc[idx, variable] * corr_factor

        _file = os.path.join(gridmet_csv_dir, '{}_{}.csv'.format(r['GFID'], variable))
        df.to_csv(_file)


if __name__ == '__main__':
    d = '/home/dgketchum/PycharmProjects/et-demands/'

    # export_openet_correction_surfaces()

    rasters_ = os.path.join(d, 'gridmet_corrected', 'correction_surfaces_aea')
    fields_shp = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample.shp')
    grimet_cent = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_gridmet_centroids.shp')
    fields_out = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample_gfid.shp')
    gridmet_dst = os.path.join(d, 'examples', 'tongue', 'climate')
    corrected_gridmet(fields_shp, grimet_cent, fields_out, gridmet_dst, rasters_, variable='eto')

# ========================= EOF ====================================================================
