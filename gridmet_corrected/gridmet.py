import os

import pandas as pd
from tqdm import tqdm
from datetime import datetime

import ee
import geopandas as gpd
from rasterstats import zonal_stats

from landsat.ee_utils import is_authorized
from gridmet_corrected.thredds import GridMet, BBox

CRS_TRANSFORM = [0.041666666666666664,
                 0, -124.78749996666667,
                 0, -0.041666666666666664,
                 49.42083333333334]

CLIMATE_COLS = {
    'etr': {
        'nc': 'agg_met_etr_1979_CurrentYear_CONUS',
        'var': 'daily_mean_reference_evapotranspiration_alfalfa',
        'col': 'etr_mm'},
    'pet': {
        'nc': 'agg_met_pet_1979_CurrentYear_CONUS',
        'var': 'daily_mean_reference_evapotranspiration_grass',
        'col': 'eto_mm'},
    'pr': {
        'nc': 'agg_met_pr_1979_CurrentYear_CONUS',
        'var': 'precipitation_amount',
        'col': 'prcp_mm'},
    'sph': {
        'nc': 'agg_met_sph_1979_CurrentYear_CONUS',
        'var': 'daily_mean_specific_humidity',
        'col': 'q_kgkg'},
    'srad': {
        'nc': 'agg_met_srad_1979_CurrentYear_CONUS',
        'var': 'daily_mean_shortwave_radiation_at_surface',
        'col': 'srad_wm2'},
    'vs': {
        'nc': 'agg_met_vs_1979_CurrentYear_CONUS',
        'var': 'daily_mean_wind_speed',
        'col': 'u10_ms'},
    'tmmx': {
        'nc': 'agg_met_tmmx_1979_CurrentYear_CONUS',
        'var': 'daily_maximum_temperature',
        'col': 'tmax_k'},
    'tmmn': {
        'nc': 'agg_met_tmmn_1979_CurrentYear_CONUS',
        'var': 'daily_minimum_temperature',
        'col': 'tmin_k'},
    'th': {
        'nc': 'agg_met_th_1979_CurrentYear_CONUS',
        'var': 'daily_mean_wind_direction',
        'col': 'wdir_deg'},
    'vpd': {
        'nc': 'agg_met_vpd_1979_CurrentYear_CONUS',
        'var': 'daily_mean_vapor_pressure_deficit',
        'col': 'vpd_kpa'}
}


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


def corrected_gridmet(fields, gridmet_points, fields_join, gridmet_csv_dir, gridmet_ras):
    """This depends on running 'Raster Pixels to Points' on a WGS Gridmet raster,
     attributing GFID, lat, and lon in the attribute table, and saving to project crs: 5071.
     GFID is an arbitrary identifier e.g., $row_number. It further depends on projecting the
     rasters to EPSG:5071, usng the project.sh bash script"""

    print('Find field-gridmet joins')

    gridmet_pts = gpd.read_file(gridmet_points)
    fields = gpd.read_file(fields)

    rasters = []
    for v in ['eto', 'etr']:
        [rasters.append(os.path.join(gridmet_ras, 'gridmet_corrected_{}_{}.tif'.format(v, m))) for m in range(1, 13)]

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
            gridmet_targets[closest_fid] = {str(m): {} for m in range(1, 13)}
            gdf = gpd.GeoDataFrame({'geometry': [closest_geo]})
            for r in rasters:
                splt = r.split('_')
                _var, month = splt[-2], splt[-1].replace('.tif', '')
                stats = zonal_stats(gdf, r, stats=['mean'])[0]['mean']
                gridmet_targets[closest_fid][month].update({_var: stats})

    fields.to_file(fields_join, crs='EPSG:5071')

    len_ = len(gridmet_targets.keys())
    print('Get gridmet for {} target points'.format(len_))
    gridmet_pts.index = gridmet_pts['GFID']
    df = pd.DataFrame()
    for k, v in tqdm(gridmet_targets.items(), total=len_):
        first = True
        for thredds_var, cols in CLIMATE_COLS.items():
            variable = cols['col']
            print(variable)
            if not thredds_var:
                continue
            r = gridmet_pts.loc[k]
            lat, lon = r['lat'], r['lon']
            g = GridMet(thredds_var, start='1987-01-01', end='2021-12-31', lat=lat, lon=lon)
            s = g.get_point_timeseries()
            df[variable] = s[thredds_var]

            if first:
                df['centroid_lat'] = [lat for _ in range(df.shape[0])]
                df['centroid_lon'] = [lat for _ in range(df.shape[0])]
                g = GridMet('elev', lat=lat, lon=lon)
                elev = g.get_point_elevation()
                df['elev_m'] = [elev for _ in range(df.shape[0])]
                first = False

        for _var in ['etr', 'eto']:
            variable = '{}_mm'.format(_var)
            for month in range(1, 13):
                corr_factor = v[str(month)][_var]
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
    corrected_gridmet(fields_shp, grimet_cent, fields_out, gridmet_dst, rasters_)

# ========================= EOF ====================================================================
