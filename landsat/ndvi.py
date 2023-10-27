import os
import sys
from datetime import datetime
from pprint import pprint

import ee
from openet import ssebop as ssebop_model

from ee_api import is_authorized
from ee_api.ee_utils import landsat_masked

sys.path.insert(0, os.path.abspath('..'))
sys.setrecursionlimit(5000)

IRR = 'projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp'

L5, L7, L8 = 'LANDSAT/LT05/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2', 'LANDSAT/LC08/C02/T1_L2'


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


if __name__ == '__main__':
    is_authorized()

    fc = ee.FeatureCollection(ee.Feature(ee.Geometry.Polygon([[-105.85544392193924, 46.105576651626485],
                                                              [-105.70747181500565, 46.105576651626485],
                                                              [-105.70747181500565, 46.222566236544104],
                                                              [-105.85544392193924, 46.222566236544104],
                                                              [-105.85544392193924, 46.105576651626485]]),
                                         {'key': 'Tongue_Ex'}))
    bucket_ = 'wudr'
    for y in [x for x in range(2016, 2022)]:
        export_ndvi(fc, y, bucket_, debug=False)

# ========================= EOF ================================================================================
