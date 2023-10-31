import os
import sys
from datetime import datetime
from tqdm import tqdm

import ee
import numpy as np
import pandas as pd
import geopandas as gpd

from rasterstats import zonal_stats

from landsat.ee_utils import landsat_masked, is_authorized

sys.path.insert(0, os.path.abspath('..'))
sys.setrecursionlimit(5000)

IRR = 'projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp'

L5, L8 = 'LANDSAT/LT05/C02/T1_L2', 'LANDSAT/LC08/C02/T1_L2'


def export_ndvi(feature_coll, year=2015, bucket=None, debug=False):
    s, e = '1987-01-01', '2021-12-31'
    irr_coll = ee.ImageCollection(IRR)
    coll = irr_coll.filterDate(s, e).select('classification')
    remap = coll.map(lambda img: img.lt(1))
    irr_min_yr_mask = remap.sum().gte(5)
    irr = irr_coll.filterDate('{}-01-01'.format(year),
                              '{}-12-31'.format(year)).select('classification').mosaic()
    irr_mask = irr_min_yr_mask.updateMask(irr.lt(1))

    coll = landsat_masked(year, feature_coll).map(lambda x: x.normalizedDifference(['B5', 'B4']))
    scenes = coll.aggregate_histogram('system:index').getInfo()

    for img_id in scenes:
        splt = img_id.split('_')
        doy = int(datetime.strptime(splt[-1], '%Y%m%d').strftime('%j'))
        if not 91 < doy < 304:
            continue
        _name = '_'.join(splt[-3:])

        img = coll.filterMetadata('system:index', 'equals', img_id).first()
        img = img.clip(feature_coll.geometry()).mask(irr_mask).multiply(1000).int()

        if debug:
            point = ee.Geometry.Point([-105.793, 46.1684])
            data = img.sample(point, 30).getInfo()
            print(data['features'])

        task = ee.batch.Export.image.toCloudStorage(
            img,
            description='NDVI_{}'.format(_name),
            bucket=bucket,
            region=feature_coll.geometry(),
            crs='EPSG:5070',
            scale=30)

        task.start()
        print(_name)


def landsat_ndvi_time_series(in_shp, tif_dir, years, out_csv):
    gdf = gpd.read_file(in_shp)
    gdf.index = gdf['FID']

    adf, first = None, True

    for yr in years:

        file_list, int_dates, doy, dts = get_list_info(tif_dir, yr)

        dt_index = pd.date_range('{}-04-01'.format(yr), '{}-10-31'.format(yr), freq='D')
        df = pd.DataFrame(index=dt_index, columns=gdf.index)

        for dt, f in tqdm(zip(dts, file_list), total=len(file_list)):
            stats = zonal_stats(in_shp, f, stats=['mean'], nodata=0.0, categorical=False, all_touched=False)
            stats = [x['mean'] if isinstance(x['mean'], float) else np.nan for x in stats]
            df.loc[dt, :] = stats

        df = df.astype(float).interpolate()
        df = df.interpolate(method='bfill')

        if first:
            adf = df.copy()
            first = False
        else:
            adf = pd.concat([adf, df], axis=0, ignore_index=False, sort=True)

    adf.to_csv(out_csv)


def get_list_info(tif_dir, year):
    """ Pass list in place of tif_dir optionally """
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
    is_authorized()

    root = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue'

    fc = ee.FeatureCollection(ee.Feature(ee.Geometry.Polygon([[-105.85544392193924, 46.105576651626485],
                                                              [-105.70747181500565, 46.105576651626485],
                                                              [-105.70747181500565, 46.222566236544104],
                                                              [-105.85544392193924, 46.222566236544104],
                                                              [-105.85544392193924, 46.105576651626485]]),
                                         {'key': 'Tongue_Ex'}))

    bucket_ = 'wudr'
    for y in [x for x in range(1987, 2016)]:
        # export_ndvi(fc, y, bucket_, debug=False)
        pass

    tif = os.path.join(root, 'landsat', 'ndvi', 'input')
    yrs = [x for x in range(2016, 2022)]
    out_js = os.path.join(root, 'landsat', 'ndvi', 'merged', '{}_{}.json'.format(yrs[0], yrs[-1]))
    shp = os.path.join(root, 'gis', 'tongue_fields_sample.shp')
    csv_ = os.path.join(root, 'landsat', 'tongue_ndvi_sample.csv')
    landsat_ndvi_time_series(shp, tif, yrs, csv_)
# ========================= EOF ================================================================================
