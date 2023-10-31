import os

import numpy as np
import pandas as pd
import geopandas as gpd


def join_gridmet_remote_sensing_daily(fields, gridmet_dir, ndvi, et_data, dst_dir):
    ndvi = pd.read_csv(ndvi, index_col=0, infer_datetime_format=True, parse_dates=True)
    start, end = ndvi.index[0], ndvi.index[-1]
    years = list(set([i.year for i in ndvi.index]))

    fields = gpd.read_file(fields)
    fields.index = fields['FID']

    et_data = pd.read_csv(et_data)
    et_data.index = et_data['FID']
    et_data = et_data.loc[fields.index]
    et_months = [(y, m) for y in years for m in range(4, 11)]
    et_cols = ['et_{}_{}'.format(y, m) for y, m in et_months]
    et_data = et_data[et_cols]
    et_index = [pd.to_datetime('{}-{}-01'.format(y, m)) for y, m in et_months]

    for f, row in fields.iterrows():
        gridmet_file = os.path.join(gridmet_dir, 'gridmet_historical_{}.csv'.format(int(row['GFID'])))
        gridmet = pd.read_csv(gridmet_file, index_col='date',
                              infer_datetime_format=True,
                              parse_dates=True).loc[start: end]

        group_my = gridmet[['eto_mm', 'etr_mm']].groupby([gridmet.index.year, gridmet.index.month]).agg('sum')
        group_my.index = pd.DatetimeIndex([pd.to_datetime('{}-{}-01'.format(y, m)) for y, m in group_my.index])
        group_my = group_my.resample('D').ffill()
        group_my = group_my.append(pd.DataFrame(index=pd.date_range(group_my.index[-1], end)[1:])).ffill()
        fractional = gridmet.loc[group_my.index, ['eto_mm', 'etr_mm']] / group_my

        et = pd.DataFrame(index=et_index, data=list(et_data.loc[f].values), columns=['et'])
        et = et.resample('D').ffill()
        et = et.append(pd.DataFrame(index=pd.date_range(et.index[-1], end)[1:])).ffill()

        gridmet['eta_r_mm'] = fractional['etr_mm'] * et['et'].values * 1000
        gridmet['eta_o_mm'] = fractional['eto_mm'] * et['et'].values * 1000
        gridmet.loc[ndvi.index, 'NDVI'] = ndvi[str(f)] / 1000

        _file = os.path.join(dst_dir, '{}_daily.csv'.format(f))
        gridmet.to_csv(_file)
        print(_file)


if __name__ == '__main__':
    d = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue'

    fields_ = os.path.join(d, 'gis', 'tongue_fields_sample_gfid.shp')
    gridmet_ = os.path.join(d, 'climate')
    ndvi_ = os.path.join(d, 'landsat', 'tongue_ndvi_sample.csv')
    et_data_ = '/media/research/IrrigationGIS/Montana/tongue/all_data.csv'
    dst_dir_ = os.path.join(d, 'field_daily')

    join_gridmet_remote_sensing_daily(fields_, gridmet_, ndvi_, et_data_, dst_dir_)

# ========================= EOF ====================================================================
