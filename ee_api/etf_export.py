import os
import sys

import ee

from ee_api.ee_utils import is_authorized

sys.path.insert(0, os.path.abspath('..'))
sys.setrecursionlimit(5000)

IRR = 'projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp'

ETF = 'projects/usgs-gee-nhm-ssebop/assets/ssebop/landsat/c02'


def export_etf(feature_coll, year=2015, bucket=None, debug=False, mask_type='irr'):
    s, e = '1987-01-01', '2021-12-31'
    irr_coll = ee.ImageCollection(IRR)
    coll = irr_coll.filterDate(s, e).select('classification')
    remap = coll.map(lambda img: img.lt(1))
    irr_min_yr_mask = remap.sum().gte(5)
    irr = irr_coll.filterDate('{}-01-01'.format(year),
                              '{}-12-31'.format(year)).select('classification').mosaic()
    irr_mask = irr_min_yr_mask.updateMask(irr.lt(1))

    coll = ee.ImageCollection(ETF).filterDate('{}-01-01'.format(year), '{}-12-31'.format(year))
    coll = coll.filterBounds(fc)
    scenes = coll.aggregate_histogram('system:index').getInfo()

    for img_id in scenes:

        splt = img_id.split('_')
        _name = '_'.join(splt[-3:])

        # if _name != 'LT05_035028_20080724':
        #     continue

        img = ee.Image(os.path.join(ETF, img_id))

        if mask_type == 'no_mask':
            img = img.clip(feature_coll.geometry()).int()
        elif mask_type == 'irr':
            img = img.clip(feature_coll.geometry()).mask(irr_mask).int()
        elif mask_type == 'inv_irr':
            img = img.clip(feature_coll.geometry()).mask(irr.gt(0)).int()

        if debug:
            point = ee.Geometry.Point([-106.576, 46.26])
            data = img.sample(point, 30).getInfo()
            print(data['features'])

        task = ee.batch.Export.image.toCloudStorage(
            img,
            description='ETF_{}'.format(_name),
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

    for mask in ['no_mask']:
        for year in list(range(2015, 2021)):
            export_etf(fc, year, bucket_, debug=False, mask_type=mask)
            pass
