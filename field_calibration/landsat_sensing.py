import os

import geopandas as gpd
import numpy as np
import pandas as pd
from rasterstats import zonal_stats
from tqdm import tqdm


def landsat_time_series(in_shp, tif_dir, years, out_csv):
    gdf = gpd.read_file(in_shp)
    gdf.index = gdf['FID']

    adf, first = None, True

    for yr in years:

        file_list, dts = get_list_info(tif_dir, yr)

        dt_index = pd.date_range('{}-01-01'.format(yr), '{}-12-31'.format(yr), freq='D')
        df = pd.DataFrame(index=dt_index, columns=gdf.index)

        print('\n', yr, len(file_list))
        for dt, f in tqdm(zip(dts, file_list), total=len(file_list)):
            stats = zonal_stats(in_shp, f, stats=['mean'], nodata=0.0, categorical=False, all_touched=False)
            stats = [x['mean'] if isinstance(x['mean'], float) else np.nan for x in stats]
            df.loc[dt, :] = stats
            df.loc[dt, :] /= 1000

        df = df.astype(float).interpolate()
        df = df.interpolate(method='bfill')

        if first:
            adf = df.copy()
            first = False
        else:
            adf = pd.concat([adf, df], axis=0, ignore_index=False, sort=True)

    adf.to_csv(out_csv)


def join_remote_sensing(_dir, dst):
    l = [os.path.join(_dir, f) for f in os.listdir(_dir) if f.endswith('.csv')]
    first = True
    params = ['etf_inv_irr',
              'ndvi_inv_irr',
              'etf_irr',
              'ndvi_irr']
    for f in l:
        param = [p for p in params if p in os.path.basename(f)][0]
        if first:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
            cols = ['{}_{}'.format(c, param) for c in df.columns]
            df.columns = cols
            first = False
        else:
            csv = pd.read_csv(f, index_col=0, parse_dates=True)
            cols = ['{}_{}'.format(c, param) for c in csv.columns]
            csv.columns = cols
            df = pd.concat([csv, df], axis=1)

    df.to_csv(dst)


def get_list_info(tif_dir, year):
    """ Pass list in place of tif_dir optionally """
    l = [os.path.join(tif_dir, x) for x in os.listdir(tif_dir) if
         x.endswith('.tif') and '_{}'.format(year) in x]
    dt_str = [f[-12:-4] for f in l]
    dates_ = [pd.to_datetime(d, format='%Y%m%d') for d in dt_str]
    tup_ = sorted([(f, d) for f, d in zip(l, dates_)], key=lambda x: x[1])
    l, dates_ = [t[0] for t in tup_], [t[1] for t in tup_]
    return l, dates_


if __name__ == '__main__':

    d = '/media/research/IrrigationGIS/et-demands'
    project = 'flynn'
    project_ws = os.path.join(d, 'examples', project)
    tables = os.path.join(project_ws, 'landsat', 'tables')

    types_ = ['inv_irr', 'irr']
    sensing_params = ['etf', 'ndvi']

    for mask_type in types_:

        for sensing_param in sensing_params:
            print('{}_{}'.format(sensing_param, mask_type))

            yrs = [x for x in range(2015, 2021)]
            shp = os.path.join(project_ws, 'gis', '{}_fields_sample.shp'.format(project))

            tif, src = None, None

            tif = os.path.join(project_ws, 'landsat', sensing_param, mask_type)
            src = os.path.join(tables, '{}_{}_{}_sample.csv'.format(project, sensing_param, mask_type))

            landsat_time_series(shp, tif, yrs, src)

    dst_ = os.path.join(project_ws, 'landsat', '{}_sensing_sample.csv'.format(project))
    join_remote_sensing(tables, dst_)

# ========================= EOF ================================================================================
