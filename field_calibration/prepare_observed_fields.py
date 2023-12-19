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


def join_gridmet_remote_sensing_daily(fields, gridmet_dir, ndvi_masked, ndvi_unmasked, etf_masked, etf_unmasked,
                                      dst_dir, overwrite=False, start_date=None, end_date=None):
    ndvi_masked = pd.read_csv(ndvi_masked, index_col=0, parse_dates=True)
    ndvi_unmasked = pd.read_csv(ndvi_unmasked, index_col=0, parse_dates=True)

    etf_masked = pd.read_csv(etf_masked, index_col=0, parse_dates=True)
    etf_unmasked = pd.read_csv(etf_unmasked, index_col=0, parse_dates=True)

    if start_date:
        ndvi_masked = ndvi_masked.loc[start_date: end_date]
        ndvi_unmasked = ndvi_unmasked.loc[start_date: end_date]
        etf_masked = etf_masked.loc[start_date: end_date]
        etf_unmasked = etf_unmasked.loc[start_date: end_date]

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
    # corrected_gridmet(fields_shp, grimet_cent, fields_gridmet, gridmet_dst, rasters_, start='2000-01-01',
    #                   end='2006-12-31')

    fields_gridmet = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample_gfid.shp')
    gridmet_ = os.path.join(d, 'examples', 'tongue', 'climate')
    ndvi_masked_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'tongue_ndvi_masked_sample.csv')
    etf_masked_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'tongue_etf_masked_sample.csv')
    ndvi_inv_mask_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'tongue_ndvi_inv_mask_sample.csv')
    etf_inv_mask_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'tongue_etf_inv_mask_sample.csv')
    et_data_ = '/media/research/IrrigationGIS/Montana/tongue/all_data.csv'
    dst_dir_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'field_daily')
    join_gridmet_remote_sensing_daily(fields_gridmet, gridmet_, ndvi_masked_, ndvi_inv_mask_, etf_masked_,
                                      etf_inv_mask_, dst_dir_, overwrite=True, start_date='2000-01-01',
                                      end_date='2006-12-31')

    fields_props = os.path.join(d, 'examples', 'tongue', 'static', 'obs', 'tongue_fields_properties.shp')
    soils_ = os.path.join(d, 'examples', 'tongue', 'gis', 'soils_aea')
    # TODO: write ndvi series to a separate file, or read the entire climate/ndvi file into the ObsCellET object
    # prepare_fields_properties(fields_gridmet, soils_, fields_props)

    cdl_ = os.path.join(d, 'examples', 'tongue', 'static', 'obs', 'tongue_sample_cdl.csv')
    cross_ = os.path.join(d, 'et-demands/prep/cdl_crosswalk_default.csv')
    out_ = os.path.join(d, 'examples', 'tongue', 'static', 'obs', 'tongue_sample_field_crops.json')
    # prep_fields_crops(cdl_, cross_, out_)

# ========================= EOF ====================================================================
