import json
import os
from subprocess import call

import geopandas as gpd
import numpy as np
import pandas as pd
from detecta import detect_peaks, detect_cusum, detect_onset
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

        print('\n', yr)
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


def detect_cuttings(ndvi_unmasked, irr_csv, out_json, ndvi_masked=None, plots=None, irr_threshold=0.5):
    unmasked = pd.read_csv(ndvi_unmasked, index_col=0, parse_dates=True)
    cols = list(unmasked.columns)
    years = list(set([i.year for i in unmasked.index]))
    irr = pd.read_csv(irr_csv, index_col=0)
    irr.drop(columns=['LAT', 'LON'], inplace=True)

    if ndvi_masked:
        masked = pd.read_csv(ndvi_masked, index_col=0, parse_dates=True)

    irrigated, fields = False, {c: {} for c in cols}
    for c in cols:
        print('\n', c)
        count, fallow = [], []

        for yr in years:

            if not c == '1786':
                continue

            if not yr == 2008:
                continue

            f_irr = irr.at[int(c), 'irr_{}'.format(yr)]
            irrigated = f_irr > irr_threshold
            if irrigated:
                df = unmasked.loc['{}-01-01'.format(yr): '{}-12-31'.format(yr), [c]]
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
                                     show=False)

                ta, tai, _, _ = detect_cusum(vals, threshold=0.100, ending=False, show=False,
                                             drift=0.005)

                onsets = detect_onset(vals, threshold=0.550, show=False)

                if plots:
                    plot_peaks(c, yr, plots)

            except ValueError:
                print('Error', yr, c)
                continue

            if not irrigated:
                fallow.append(yr)
                continue

            irr_doys = []
            green_start_dates, cut_dates = [], []
            green_start_doys, cut_doys = [], []
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
                        green_start_doys.append(date)
                        dts = '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day)
                        green_start_dates.append(dts)

                for pk in peaks:

                    on_peak = False
                    if np.any(np.array([ons[0] < pk < ons[1] for ons in onsets])):
                        on_peak = True

                    if on_peak:
                        date = df.index[pk]
                        cut_doys.append(date)
                        dts = '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day)
                        cut_dates.append(dts)

                irr_doys = [[i for i in range(s.dayofyear, e.dayofyear)] for s, e in zip(green_start_doys, cut_doys)]
                irr_doys = list(np.array(irr_doys, dtype=object).flatten())
                irr_windows = [(gu, cd) for gu, cd in zip(green_start_dates, cut_dates)]

                if not irr_windows:
                    roll = pd.DataFrame((diff.rolling(window=15).mean() > 0.0), columns=[c])
                    roll = roll.loc[[i for i in roll.index if 3 < i.month < 11]]
                    roll['crossing'] = (roll[c] != roll[c].shift()).cumsum()
                    roll['count'] = roll.groupby([c, 'crossing']).cumcount(ascending=True)
                    irr_doys = [i.dayofyear for i in roll[roll[c]].index]
                    roll = roll[(roll['count'] == 0 & roll[c])]
                    start_idx, end_idx = list(roll.loc[roll[c] == 1].index), list(roll.loc[roll[c] == 0].index)
                    start_idx = ['{}-{:02d}-{:02d}'.format(d.year, d.month, d.day) for d in start_idx]
                    end_idx = ['{}-{:02d}-{:02d}'.format(d.year, d.month, d.day) for d in end_idx]
                    irr_windows = [(s, e) for s, e in zip(start_idx, end_idx)]

            else:
                irr_windows = []

            count.append(len(pk_dates))

            green_start_dates = list(np.unique(np.array(green_start_dates)))

            fields[c][yr] = {'pk_count': len(pk_dates),
                             'green_ups': green_start_dates,
                             'cut_dates': cut_dates,
                             'irr_windows': irr_windows,
                             'irr_doys': irr_doys,
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

    domain = 'flynn'
    root = '/home/dgketchum/PycharmProjects/et-demands/examples/{}'.format(domain)
    drive = '/media/research/IrrigationGIS/Montana/tongue/et_demands'

    types_ = ['irr']
    sensing_param = 'ETF'

    for mask_type in types_:

        yrs = [x for x in range(2015, 2022)]
        shp = os.path.join(root, 'gis', '{}_fields_sample.shp'.format(domain))

        tif, src = None, None

        if mask_type == 'irr':
            tif = os.path.join(drive, domain, sensing_param, 'input_masked')
            src = os.path.join(root, 'landsat', '{}_{}_masked_sample.csv'.format(domain, sensing_param))
        elif mask_type == 'inv_irr':
            tif = os.path.join(drive, domain, sensing_param, 'input_inv_mask')
            src = os.path.join(root, 'landsat', '{}_{}_inv_mask_sample.csv'.format(domain, sensing_param))

        landsat_time_series(shp, tif, yrs, src)


# ========================= EOF ================================================================================
