import os
import sys
import json
from datetime import datetime
from tqdm import tqdm

import ee
import numpy as np
import pandas as pd
import geopandas as gpd

import matplotlib.pyplot as plt

from rasterstats import zonal_stats

from detecta import detect_peaks, detect_cusum

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
        if 91 < doy < 304:
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

        file_list, dts = get_list_info(tif_dir, yr)

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
    dt_str = [f[-12:-4] for f in l]
    dates_ = [pd.to_datetime(d, format='%Y%m%d') for d in dt_str]
    tup_ = sorted([(f, d) for f, d in zip(l, dates_)], key=lambda x: x[1])
    l, dates_ = [t[0] for t in tup_], [t[1] for t in tup_]
    return l, dates_


def plot_ndvi(csv, plot_dir):
    adf = pd.read_csv(csv, index_col=0, parse_dates=True, infer_datetime_format=True)
    cols = list(adf.columns)
    years = list(set([i.year for i in adf.index]))

    for c in cols:
        df = adf[[c]]
        df['date'] = df.index
        df['year'] = df.date.dt.year
        df['date'] = df.date.dt.strftime('%m-%d')
        df.index = [x for x in range(0, df.shape[0])]
        df = df.set_index(['year', 'date'])[c].unstack(-2)
        df.dropna(axis=1, how='all', inplace=True)

        colors = ['#' + ''.join(np.random.choice(list('0123456789ABCDEF'), size=6)) for _ in df.columns]
        ax = df.plot(logy=False, legend=False, alpha=0.8, color=colors, ylabel='NDVI',
                     title='NDVI {} - {}'.format(years[0], years[-1]), figsize=(30, 10))

        df = df.loc[df.index != '02-29']
        df.dropna(how='any', axis=1, inplace=True)

        _file = os.path.join(plot_dir, 'ndvi_{}.png'.format(c))
        plt.savefig(_file)
        # plt.show()
        print(_file)


def detect_cuttings(csv, out_json):
    adf = pd.read_csv(csv, index_col=0, parse_dates=True, infer_datetime_format=True)
    cols = list(adf.columns)
    diff = adf.diff()
    years = list(set([i.year for i in adf.index]))

    fields = {c: {} for c in cols}
    for c in cols:
        count, fallow = [], []

        for yr in years:

            df = adf.loc['{}-01-01'.format(yr): '{}-12-31'.format(yr), [c]]
            vals = df.values
            try:
                y_change = detect_cusum(vals, threshold=300, ending=False, drift=5, show=False)
            except ValueError:
                print(yr, c)
                continue
            if len(y_change[0]) == 0:
                fallow.append(yr)
                continue
            peaks, inflections = y_change[1], y_change[0]
            inflect_dates = [df.index[i] for i in inflections]
            irr_dates, cut_dates, pk_dates = [], [], []

            for j, (i, p) in enumerate(zip(inflections, peaks)):

                sign = diff.loc[diff.index[p], c]
                if sign < 0:
                    dt = df.index[p]
                    pk_dates.append((p.item(), '{}-{}'.format(dt.month, dt.day)))

                sign = diff.loc[diff.index[i], c]

                if sign < 0:
                    dt = inflect_dates[j]
                    cut_dates.append((i.item(), '{}-{}'.format(dt.month, dt.day)))
                else:
                    dt = inflect_dates[j]
                    irr_dates.append((i.item(), '{}-{}'.format(dt.month, dt.day)))

            count.append(len(pk_dates))
            fields[c][yr] = {'pk_count': len(pk_dates), 'peak_dates': pk_dates,
                             'irr_dates': irr_dates, 'cut_dates': cut_dates}

        avg_ct = np.array(count).mean()
        fields[c]['average_cuttings'] = float(avg_ct)
        fields[c]['fallow_years'] = fallow

    with open(out_json, 'w') as fp:
        json.dump(fields, fp, indent=4)


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
    for y in [x for x in range(1987, 2022)]:
        # export_ndvi(fc, y, bucket_, debug=False)
        pass

    tif = os.path.join(root, 'landsat', 'ndvi', 'input')
    yrs = [x for x in range(1987, 2022)]
    out_js = os.path.join(root, 'landsat', 'ndvi', 'merged', '{}_{}.json'.format(yrs[0], yrs[-1]))
    shp = os.path.join(root, 'gis', 'tongue_fields_sample.shp')
    csv_ = os.path.join(root, 'landsat', 'tongue_ndvi_sample_.csv')
    # landsat_ndvi_time_series(shp, tif, yrs, csv_)

    plot_dir_ = './plots'
    # plot_ndvi(csv_, plot_dir_)

    js_ = os.path.join(root, 'landsat', 'tongue_ndvi_cuttings.json')
    detect_cuttings(csv_, js_)
# ========================= EOF ================================================================================
