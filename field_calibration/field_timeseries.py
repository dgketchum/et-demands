import os

import geopandas as gpd
import pandas as pd

from gridmet_corrected.gridmet import corrected_gridmet


def join_gridmet_remote_sensing_daily(fields, gridmet_dir, ndvi_masked, ndvi_unmasked, etf_masked, etf_unmasked,
                                      dst_dir, overwrite=False, start_date=None, end_date=None):



    start, end = ndvi_masked.index[0], ndvi_masked.index[-1]

    fields = gpd.read_file(fields)
    fields.index = fields['FID']

    for f, row in fields.iterrows():

        _file = os.path.join(dst_dir, '{}_daily.csv'.format(f))
        if os.path.exists(_file) and not overwrite:
            continue

        gridmet_file = os.path.join(gridmet_dir, 'gridmet_historical_{}.csv'.format(int(row['GFID'])))
        gridmet = pd.read_csv(gridmet_file, index_col='date', parse_dates=True).loc[start: end]

        gridmet.loc[ndvi_masked.index, 'NDVI_IRR'] = ndvi_masked[str(f)]
        gridmet.loc[ndvi_unmasked.index, 'NDVI_NO_IRR'] = ndvi_unmasked[str(f)]
        gridmet.loc[etf_masked.index, 'ETF_IRR'] = etf_masked[str(f)]
        gridmet.loc[etf_unmasked.index, 'ETF_NO_IRR'] = etf_unmasked[str(f)]

        if start_date:
            gridmet = gridmet.loc[start_date:]
        if end_date:
            gridmet = gridmet.loc[:end_date]

        gridmet.to_csv(_file)
        print(_file)


if __name__ == '__main__':

    d = '/media/research/IrrigationGIS/et-demands'
    project = 'flynn'
    project_ws = os.path.join(d, 'examples', project)

    gridmet = os.path.join(d, 'gridmet')
    rasters_ = os.path.join(gridmet, 'gridmet_corrected', 'correction_surfaces_aea')
    grimet_cent = os.path.join(gridmet, 'gridmet_centroids.shp')

    fields_shp = os.path.join(project_ws, 'gis', '{}_fields_sample.shp'.format(project))
    fields_gridmet = os.path.join(project_ws, 'gis', '{}_fields_sample_gfid.shp'.format(project))
    met = os.path.join(project_ws, 'timeseries')
    corrected_gridmet(fields_shp, grimet_cent, fields_gridmet, met, rasters_, start='2015-01-01',
                      end='2020-12-31')

    landsat = os.path.join(d, 'landsat')
    dst_dir_ = os.path.join(project_ws, 'input_timeseries')
    join_gridmet_remote_sensing_daily(fields_gridmet, met, landsat, dst_dir_, overwrite=True, start_date='2015-01-01',
                                      end_date='2020-12-31')
# ========================= EOF ====================================================================
