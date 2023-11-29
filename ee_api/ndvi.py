import os
import sys
import json
from subprocess import call
from datetime import datetime
from tqdm import tqdm

import ee
import numpy as np
import pandas as pd
import geopandas as gpd

import matplotlib.pyplot as plt

from rasterstats import zonal_stats

from detecta import detect_peaks, detect_cusum, detect_onset

from ee_api.ee_utils import landsat_masked, is_authorized

sys.path.insert(0, os.path.abspath('..'))
sys.setrecursionlimit(5000)

IRR = 'projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp'

L5, L8 = 'LANDSAT/LT05/C02/T1_L2', 'LANDSAT/LC08/C02/T1_L2'


def export_ndvi(feature_coll, year=2015, bucket=None, debug=False, mask_type='irr'):
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
        _name = '_'.join(splt[-3:])

        # if _name != 'LT05_035028_20080724':
        #     continue

        img = coll.filterMetadata('system:index', 'equals', img_id).first()

        if mask_type == 'no_mask':
            img = img.clip(feature_coll.geometry()).multiply(1000).int()
        elif mask_type == 'irr':
            img = img.clip(feature_coll.geometry()).mask(irr_mask).multiply(1000).int()
        elif mask_type == 'inv_irr':
            img = img.clip(feature_coll.geometry()).mask(irr.gt(0)).multiply(1000).int()

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

        dt_index = pd.date_range('{}-01-01'.format(yr), '{}-12-31'.format(yr), freq='D')
        df = pd.DataFrame(index=dt_index, columns=gdf.index)

        print('\n', yr, '\n')
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


def detect_cuttings(ndvi_masked, ndvi_unmasked, irr_csv, out_json, plots=None, irr_threshold=0.5):
    masked = pd.read_csv(ndvi_masked, index_col=0, parse_dates=True, infer_datetime_format=True)
    unmasked = pd.read_csv(ndvi_unmasked, index_col=0, parse_dates=True, infer_datetime_format=True)
    cols = list(masked.columns)
    years = list(set([i.year for i in masked.index]))
    irr = pd.read_csv(irr_csv, index_col=0)
    irr.drop(columns=['LAT', 'LON'], inplace=True)

    irrigated, fields = False, {c: {} for c in cols}
    for c in cols:
        print('\n', c)
        count, fallow = [], []

        for yr in years:

            # if not c == '1779':
            #     continue
            #
            # if not yr == 2004:
            #     continue

            f_irr = irr.at[int(c), 'irr_{}'.format(yr)]
            irrigated = f_irr > irr_threshold
            if irrigated:
                df = masked.loc['{}-01-01'.format(yr): '{}-12-31'.format(yr), [c]]
            else:
                df = unmasked.loc['{}-01-01'.format(yr): '{}-12-31'.format(yr), [c]]

            diff = df.diff()

            nan_ct = np.count_nonzero(np.isnan(df.values))
            if nan_ct > 200:
                print('{}: {} has {}/{} nan'.format(c, yr, nan_ct, df.values.size))
                continue

            vals = df.values

            try:

                if plots:
                    peak_fig = os.path.join(plots, '{}_{}_pk.png'.format(c, yr))
                    cumsum_fig = os.path.join(plots, '{}_{}_cs.png'.format(c, yr))
                    onset_fig = os.path.join(plots, '{}_{}_ons.png'.format(c, yr))
                else:
                    peak_fig, cumsum_fig, onset_fig = None, None, None

                # locally modified each detecta function to customize plotting
                peaks = detect_peaks(vals.flatten(), mph=0.500, mpd=30, threshold=0, valley=False,
                                     show=False, save_fig=peak_fig)

                ta, tai, _, _ = detect_cusum(vals, threshold=0.100, ending=False, show=False,
                                             drift=0.005, save_fig=cumsum_fig)

                onsets = detect_onset(vals, threshold=0.550, show=False, save_fig=onset_fig)

                if plots:
                    plot_peaks(c, yr, plots)

            except ValueError:
                print('Error', yr, c)
                continue

            if not irrigated:
                fallow.append(yr)
                continue

            green_start_dates, cut_dates = [], []
            irr_dates, cut_dates, pk_dates = [], [], []

            if irrigated:
                for infl, green in zip(ta, tai):

                    off_peak = False
                    if np.all(~np.array([ons[0] < green < ons[1] for ons in onsets])):
                        off_peak = True

                    if not off_peak:
                        continue

                    sign = diff.loc[diff.index[green + 1]: diff.index[green + 10], c].mean()

                    if sign > 0:
                        date = df.index[green]
                        dts = '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day)
                        green_start_dates.append(dts)

                for pk in peaks:

                    on_peak = False
                    if np.any(np.array([ons[0] < pk < ons[1] for ons in onsets])):
                        on_peak = True

                    if on_peak:
                        date = df.index[pk]
                        dts = '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day)
                        cut_dates.append(dts)

                irr_windows = [(gu, cd) for gu, cd in zip(green_start_dates, cut_dates)]

                if not irr_windows:
                    roll = pd.DataFrame((diff.rolling(window=15).mean() > 0.0), columns=[c])
                    roll = roll.loc[[i for i in roll.index if 3 < i.month < 11]]
                    roll['crossing'] = (roll[c] != roll[c].shift()).cumsum()
                    roll['count'] = roll.groupby([c, 'crossing']).cumcount(ascending=True)
                    roll = roll[(roll['count'] == 0 & roll[c])]
                    start_idx, end_idx = list(roll.loc[roll[c] == 1].index), list(roll.loc[roll[c] == 0].index)
                    start_idx = ['{}-{:02d}-{:02d}'.format(d.year, d.month, d.day) for d in start_idx]
                    end_idx = ['{}-{:02d}-{:02d}'.format(d.year, d.month, d.day) for d in end_idx]
                    irr_windows = [(s, e) for s, e in zip(start_idx, end_idx)]

                elif plots:
                    os.remove(os.path.join(plots, '{}_{}_comb.png'.format(c, yr)))

            else:
                irr_windows = []

            count.append(len(pk_dates))
            fields[c][yr] = {'pk_count': len(pk_dates),
                             'green_ups': green_start_dates,
                             'cut_dates': cut_dates,
                             'irr_windows': irr_windows,
                             'irrigated': int(irrigated),
                             'f_irr': f_irr}

        avg_ct = np.array(count).mean()
        fields[c]['average_cuttings'] = float(avg_ct)
        fields[c]['fallow_years'] = fallow

    with open(out_json, 'w') as fp:
        json.dump(fields, fp, indent=4)


def plot_peaks(field_id, yr, plots):
    ons = os.path.join(plots, '{}_{}_ons.png'.format(field_id, yr))
    pk = os.path.join(plots, '{}_{}_pk.png'.format(field_id, yr))
    cs = os.path.join(plots, '{}_{}_cs.png'.format(field_id, yr))

    cmd = ['convert', '-append',
           ons, pk, cs,
           os.path.join(plots, '{}_{}_comb.png'.format(field_id, yr))]

    call(cmd)

    [os.remove(f) for f in [ons, pk, cs]]


if __name__ == '__main__':
    is_authorized()
    bucket_ = 'wudr'

    root = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue'

    fc = ee.FeatureCollection(ee.Feature(ee.Geometry.Polygon([[-105.85544392193924, 46.105576651626485],
                                                              [-105.70747181500565, 46.105576651626485],
                                                              [-105.70747181500565, 46.222566236544104],
                                                              [-105.85544392193924, 46.222566236544104],
                                                              [-105.85544392193924, 46.105576651626485]]),
                                         {'key': 'Tongue_Ex'}))

    types_ = ['no_mask', 'irr', 'inv_irr']

    for mask_type in types_:

        for y in [x for x in range(1987, 2022)]:
            # export_ndvi(fc, y, bucket_, debug=False, mask_type=mask_type)
            pass

        yrs = [x for x in range(1987, 2022)]
        shp = os.path.join(root, 'gis', 'tongue_fields_sample.shp')

        tif, ndvi_src = None, None
        if mask_type == 'no_mask':
            tif = os.path.join(root, 'landsat', 'ndvi', 'input_unmasked')
            ndvi_src = os.path.join(root, 'landsat', 'tongue_ndvi_unmasked_sample.csv')
        elif mask_type == 'irr':
            tif = os.path.join(root, 'landsat', 'ndvi', 'input_masked')
            ndvi_src = os.path.join(root, 'landsat', 'tongue_ndvi_masked_sample.csv')
        elif mask_type == 'inv_irr':
            tif = os.path.join(root, 'landsat', 'ndvi', 'input_inv_mask')
            ndvi_src = os.path.join(root, 'landsat', 'tongue_ndvi_inv_mask_sample.csv')

        # landsat_ndvi_time_series(shp, tif, yrs, ndvi_src)

    plot_dir_ = './plots'
    # plot_ndvi(csv_, plot_dir_)

    irr_ = os.path.join(root, 'landsat', 'tongue_sample_irr.csv')
    js_ = os.path.join(root, 'landsat', 'tongue_ndvi_cuttings.json')
    figs = '/home/dgketchum/Downloads/cuttings'

    masked_ = os.path.join(root, 'landsat', 'tongue_ndvi_masked_sample.csv')
    ndvi_inv_mask_ = os.path.join(root, 'landsat', 'tongue_ndvi_inv_mask_sample.csv')

    detect_cuttings(masked_, ndvi_inv_mask_, irr_, js_, plots=None, irr_threshold=0.5)
# ========================= EOF ================================================================================
