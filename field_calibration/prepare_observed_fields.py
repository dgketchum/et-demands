import os
import json

import numpy as np
import pandas as pd
import geopandas as gpd

from gridmet_corrected.gridmet import corrected_gridmet


def prepare_fields_properties(met_fields, soils, fields_out):
    fields = gpd.read_file(met_fields)
    fields.index = fields['FID']

    awc = gpd.read_file(os.path.join(soils, 'AWC_WTA_0to152cm_statsgo.shp'), mask=fields)
    clay = gpd.read_file(os.path.join(soils, 'Clay_WTA_0to152cm_statsgo.shp'), mask=fields)
    sand = gpd.read_file(os.path.join(soils, 'Sand_WTA_0to152cm_statsgo.shp'), mask=fields)

    acreage_field = 'AG_ACRES'

    for param, var_ in zip(['AWC', 'Clay', 'Sand'], [awc, clay, sand]):
        intersect = gpd.overlay(var_, fields, how='intersection')
        intersect['area'] = [g.area for g in intersect['geometry']]
        area_wght_awc = []
        for fid, row in fields.iterrows():
            inter = intersect[intersect['FID'] == fid]
            tot_area = inter['area'].sum()
            area_wght_awc.append((inter[param] * inter['area'] / tot_area).sum())

        if param == 'AWC':
            fields['AWC_IN_FT'] = list(np.array(area_wght_awc) * 12)

        fields[param.upper()] = area_wght_awc

    fields['HYDGRP_NUM'] = None
    fields['HYDGRP'] = None

    # these appear to be defaults
    fields['aridity_rating'] = [50 for _ in range(fields.shape[0])]
    fields['soil_depth'] = [60 for _ in range(fields.shape[0])]
    fields['permeability'] = [-999 for _ in range(fields.shape[0])]

    for fid, row in fields.iterrows():
        if row['SAND'] > 50:
            fields.loc[fid, 'HYDGRP_NUM'], fields.loc[fid, 'HYDGRP'] = 1, 'A'
        elif row['CLAY'] > 40:
            fields.loc[fid, 'HYDGRP_NUM'], fields.loc[fid, 'HYDGRP'] = 3, 'C'
        else:
            fields.loc[fid, 'HYDGRP_NUM'], fields.loc[fid, 'HYDGRP'] = 2, 'B'

    fields.to_file(fields_out, index=False)
    fields.drop(columns=['geometry'], inplace=True)
    fields = pd.DataFrame(fields)
    fields.to_csv(fields_out.replace('.shp', '.csv'))


def join_gridmet_remote_sensing_daily(fields, gridmet_dir, ndvi, ndvi_dst, et_data, dst_dir):
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

        ndvi_field = gridmet[['NDVI']].copy()
        nd_file = os.path.join(ndvi_dst, '{}_daily.csv'.format(f))
        ndvi_field.to_csv(nd_file)

        _file = os.path.join(dst_dir, '{}_daily.csv'.format(f))
        gridmet.to_csv(_file)
        print(_file)


def prep_fields_crops(cdl, crosswalk_path, out):

    dct = {}

    df = pd.read_csv(cdl)
    df.index = df['FID']
    cols = [c for c in df.columns if 'crop' in c]
    years = [int(c.split('_')[1]) for c in cols]
    df = df[cols].astype(int)

    cross_df = pd.read_csv(crosswalk_path)
    cross_dict = dict()
    for index, row in cross_df.iterrows():
        cross_dict[row.cdl_no] = list(map(int, str(row.etd_no).split(',')))

    for fid, crops in df.iterrows():
        etd = []
        for v in crops.values:
            try:
                etd.append(int(cross_dict[v][0]))
            except KeyError:
                etd.append(0)

        dct[fid] = {'cdl': [v.item() for v in crops.values], 'years': years,
                    'etd': etd}

    with open(out, 'w') as fp:
        json.dump(dct, fp, indent=4)


if __name__ == '__main__':
    d = '/home/dgketchum/PycharmProjects/et-demands'

    rasters_ = os.path.join(d, 'gridmet_corrected', 'correction_surfaces_aea')
    fields_shp = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample.shp')
    grimet_cent = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_gridmet_centroids.shp')
    fields_gridmet = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample_gfid.shp')
    gridmet_dst = os.path.join(d, 'examples', 'tongue', 'climate')
    # corrected_gridmet(fields_shp, grimet_cent, fields_gridmet, gridmet_dst, rasters_)

    fields_gridmet = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample_gfid.shp')
    gridmet_ = os.path.join(d, 'examples', 'tongue', 'climate')
    ndvi_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'tongue_ndvi_sample.csv')
    ndvi_out = os.path.join(d, 'examples', 'tongue', 'landsat', 'ndvi', 'field_daily')
    et_data_ = '/media/research/IrrigationGIS/Montana/tongue/all_data.csv'
    dst_dir_ = os.path.join(d, 'examples', 'tongue', 'field_daily')
    # join_gridmet_remote_sensing_daily(fields_gridmet, gridmet_, ndvi_, ndvi_out, et_data_, dst_dir_)

    fields_gridmet = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample_gfid.shp')
    fields_props = os.path.join(d, 'examples', 'tongue', 'static', 'obs', 'tongue_fields_properties.shp')
    soils_ = os.path.join(d, 'examples', 'tongue', 'gis', 'soils_aea')
    # TODO: write ndvi series to a separate file, or read the entire climate/ndvi file into the ObsCellET object
    # prepare_fields_properties(fields_gridmet, soils_, fields_props)

    cdl_ = os.path.join(d, 'examples', 'tongue', 'static', 'obs', 'tongue_sample_cdl.csv')
    cross_ = os.path.join(d, 'et-demands/prep/cdl_crosswalk_default.csv')
    out_ = os.path.join(d, 'examples', 'tongue', 'static', 'obs', 'tongue_sample_field_crops.json')
    prep_fields_crops(cdl_, cross_, out_)

# ========================= EOF ====================================================================
