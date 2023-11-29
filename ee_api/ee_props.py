import ee

IRR = 'projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp'


def get_cdl(fields, desc):
    plots = ee.FeatureCollection(fields)
    crops, first = None, True
    cdl_years = [x for x in range(2008, 2023)]

    _selectors = ['FID', 'LAT', 'LON']

    for y in cdl_years:

        image = ee.Image('USDA/NASS/CDL/{}'.format(y))
        crop = image.select('cropland')
        _name = 'crop_{}'.format(y)
        _selectors.append(_name)
        if first:
            crops = crop.rename(_name)
            first = False
        else:
            crops = crops.addBands(crop.rename(_name))

    modes = crops.reduceRegions(collection=plots,
                                reducer=ee.Reducer.mode(),
                                scale=30)

    out_ = '{}_cdl'.format(desc)
    task = ee.batch.Export.table.toCloudStorage(
        modes,
        description=out_,
        bucket='wudr',
        fileNamePrefix=out_,
        fileFormat='CSV',
        selectors=_selectors)

    task.start()


def get_irrigation(fields, desc, debug=False):
    plots = ee.FeatureCollection(fields)

    s, e = '1987-01-01', '2021-12-31'

    irr_coll = ee.ImageCollection(IRR)
    remap = irr_coll.map(lambda img: img.lt(1))

    _selectors = ['FID', 'LAT', 'LON']
    first = True

    area, irr_img = ee.Image.pixelArea(), None

    for year in range(1987, 2022):

        irr = irr_coll.filterDate('{}-01-01'.format(year),
                                  '{}-12-31'.format(year)).select('classification').mosaic()

        irr = irr.lt(1)

        _name = 'irr_{}'.format(year)
        _selectors.append(_name)

        if first:
            irr_img = irr.rename(_name)
            first = False
        else:
            irr_img = irr_img.addBands(irr.rename(_name))

    means = irr_img.reduceRegions(collection=plots,
                                  reducer=ee.Reducer.mean(),
                                  scale=30)

    if debug:
        debug = means.filterMetadata('FID', 'equals', 1789).getInfo()

    task = ee.batch.Export.table.toCloudStorage(
        means,
        description=desc,
        bucket='wudr',
        fileNamePrefix=desc,
        fileFormat='CSV',
        selectors=_selectors)

    task.start()


if __name__ == '__main__':
    ee.Initialize()

    fields_ = 'users/dgketchum/fields/tongue_sample'
    description = 'tongue_sample_irr'
    # get_cdl(fields_, description)

    get_irrigation(fields_, description, debug=False)

# ========================= EOF ====================================================================