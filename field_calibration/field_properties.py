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
    d = '/media/research/IrrigationGIS/et-demands'
    project = 'flynn'
    project_ws = os.path.join(d, 'examples', project)

    fields_props = os.path.join(project_ws, 'static', 'obs', 'tongue_fields_properties.shp')
    soils_ = '/media/research/IrrigationGIS/Montana/tongue/et_demands/gis/soils_aea'
    # prepare_fields_properties(fields_gridmet, soils_, fields_props)

    cdl_ = os.path.join(project_ws, 'static', 'obs', 'tongue_sample_cdl.csv')
    cross_ = os.path.join(project_ws, 'static', 'cdl_crosswalk_default.csv')
    out_ = os.path.join(project_ws, 'static', 'obs', 'tongue_sample_field_crops.json')
    # prep_fields_crops(cdl_, cross_, out_)

# ========================= EOF ====================================================================
