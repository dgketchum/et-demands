import os

from tqdm import tqdm

import numpy as np
import pandas as pd
import geopandas as gpd

from rasterstats import zonal_stats


def landsat_ndvi_time_series(in_shp, tif_dir, year, out_csv):
    gdf = gpd.read_file(in_shp)
    gdf.index = gdf['FID']

    file_list, int_dates, doy, dts = get_list_info(tif_dir, year, list_=False)

    dt_index = pd.date_range('{}-04-01'.format(year), '{}-10-31'.format(year), freq='D')
    df = pd.DataFrame(index=dt_index, columns=gdf.index)

    for dt, f in tqdm(zip(dts, file_list), total=len(file_list)):
        stats = zonal_stats(in_shp, f, stats=['std'], nodata=0.0, categorical=False, all_touched=False)
        stats = [x['std'] if isinstance(x['std'], float) else np.nan for x in stats]
        df.loc[dt, :] = stats

    df = df.astype(float).interpolate()
    df = df.interpolate(method='bfill')
    df.to_csv(out_csv)


def get_list_info(tif_dir, year, list_=False):
    """ Pass list in place of tif_dir optionally """
    if list_:
        l = tif_dir
    else:
        l = [os.path.join(tif_dir, x) for x in os.listdir(tif_dir) if
             x.endswith('.tif') and '_{}'.format(year) in x]
    srt = sorted([x for x in l], key=lambda x: int(x.split('.')[0][-4:]))
    d = [x.split('.')[0][-8:] for x in srt]
    d_numeric = [int(x) for x in d]
    dstr = ['{}-{}-{}'.format(x[:4], x[4:6], x[-2:]) for x in d]
    dates_ = [pd.to_datetime(x) for x in dstr]
    doy = [int(dt.strftime('%j')) for dt in dates_]
    return l, d_numeric, doy, dates_


if __name__ == '__main__':
    root = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue'

    tif = os.path.join(root, 'landsat', 'ndvi', 'input')
    yr = 2016
    out_js = os.path.join(root, 'landsat', 'ndvi', 'merged', '{}.json'.format(yr))
    shp = os.path.join(root, 'gis', 'tongue_fields_sample.shp')
    csv_ = os.path.join(root, 'gis', 'tongue_ndvi_sample.csv')
    landsat_ndvi_time_series(shp, tif, yr, csv_)
# ========================= EOF ====================================================================
