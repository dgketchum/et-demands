import os
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
            description='NDVI_{}_{}'.format(mask_type, _name),
            bucket=bucket,
            region=feature_coll.geometry(),
            crs='EPSG:5070',
            scale=30)

        task.start()
        print(_name)


if __name__ == '__main__':
    is_authorized()
    bucket_ = 'wudr'

    fc = ee.FeatureCollection(ee.Feature(ee.Geometry.Polygon([[-106.63372199162623, 46.235698473362476],
                                                              [-106.49124304875514, 46.235698473362476],
                                                              [-106.49124304875514, 46.31472036075997],
                                                              [-106.63372199162623, 46.31472036075997],
                                                              [-106.63372199162623, 46.235698473362476]]),
                                         {'key': 'Flynn_Ex'}))

    types_ = ['irr']

    for mask_type in types_:

        for y in [x for x in range(1987, 2021)]:
            export_ndvi(fc, y, bucket_, debug=False, mask_type=mask_type)
            pass
# ========================= EOF ====================================================================
