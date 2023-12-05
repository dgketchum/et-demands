# --------------------------------
# Name:         image_wrs2_export.py
# Purpose:      Export ETf images
# --------------------------------

import argparse
from builtins import input
from calendar import monthrange
import datetime
from functools import reduce
import json
import logging
import math
from operator import mul
import os
import pprint
import re
import sys
import time

import ee
from osgeo import ogr, osr

# This is an awful way of getting the parent folder into the path
# We really should package this up as a module with a setup.py
# This way the ssebop-gee and openet folders would be in the
#   PYTHONPATH env. variable
ssebop_gee_path = os.path.dirname(os.path.dirname(
    os.path.abspath(os.path.realpath(__file__))))
sys.path.insert(0, os.path.join(ssebop_gee_path, 'openet'))
sys.path.insert(0, ssebop_gee_path)
import openet.common as common
import openet.inputs as inputs
import openet.landsat as landsat
import openet.utils as utils
import ssebop.ssebop as ssebop


def main(ini_path=None, start=None, end=None, scale=None,
         irrigation_mask='no_mask', clip=None, debug=False):
    """Export daily ETf images as EE assets or to Google Drive

    Args:
        irr_mask:
        scale:
        end:
        start:
        ini_path (str): Input file path
        overwrite_flag (bool): if True, overwrite existing files

    Returns:
        None
    """
    logging.info('\nComputing daily ETf images')  # Read config file
    ini = inputs.read(ini_path)

    #  hack to modify start and end dates
    ini['INPUTS']['start_date'] = start
    ini['INPUTS']['end_date'] = end
    inputs.parse_section(ini, section='INPUTS')
    inputs.parse_section(ini, section='EXPORT')
    inputs.parse_section(ini, section=ini['INPUTS']['et_model'])
    logging.info('\nInitializing Earth Engine')
    ee.Initialize()

    # Remove scene_id product
    if 'scene_id' in ini['EXPORT']['products']:
        ini['EXPORT']['products'].remove('scene_id')

    # Get list of WRS2 tiles that intersect the study area
    logging.debug('\nBuilding export list')
    export_list = list(wrs2_tile_export_generator(
        ini['INPUTS']['study_area_path'],
        wrs2_coll=ini['INPUTS']['wrs2_coll'],
        cell_size=ini['EXPORT']['cell_size'],
        output_crs=ini['EXPORT']['output_crs'],
        output_osr=ini['EXPORT']['output_osr'],
        wrs2_tile_list=ini['INPUTS']['wrs2_tiles'],
        wrs2_tile_field=ini['INPUTS']['wrs2_tile_field'],
        snap_x=ini['EXPORT']['snap_x'],
        snap_y=ini['EXPORT']['snap_y'],
        wrs2_buffer=ini['INPUTS']['wrs2_buffer']))

    if not export_list:
        logging.error('\nEmpty export list, exiting')
        return False

    # Process each WRS2 tile separately
    logging.info('\nImage Exports')
    for export_n, export_info in enumerate(export_list):
        # path, row = map(int, path_row_re.findall(export_info['index'])[0])
        logging.info('WRS2 tile: {}  ({}/{})'.format(
            export_info['index'], export_n + 1, len(export_list)))

        logging.debug('  Shape:     {}'.format(export_info['shape']))
        logging.debug('  Transform: {}'.format(export_info['geo']))
        logging.debug('  Extent:    {}'.format(export_info['extent']))
        logging.debug('  MaxPixels: {}'.format(export_info['maxpixels']))

        # Get the full Landsat collection
        logging.debug('  Getting image IDs from EarthEngine')
        landsat_coll = landsat.get_landsat_coll(
            wrs2_tile_list=export_info['wrs2_tiles'],
            cloud_cover=ini['INPUTS']['cloud_cover'],
            start_date=ini['INPUTS']['start_date'],
            end_date=ini['INPUTS']['end_date'],
            landsat5_flag=ini['INPUTS']['landsat5_flag'],
            landsat7_flag=ini['INPUTS']['landsat7_flag'],
            landsat8_flag=ini['INPUTS']['landsat8_flag'],
        )
        scene_id_list = landsat_coll.aggregate_histogram('SCENE_ID') \
            .getInfo().keys()
        if not scene_id_list:
            logging.info('\nNo Landsat images in date range, exiting')
            return start

        for scene_id in scene_id_list:

            # if scene_id != 'LT05_035028_19870901':
            #     continue

            logging.info('{}'.format(scene_id))
            l, p, r, year, month, day = landsat.parse_landsat_id(scene_id)
            image_dt = datetime.datetime.strptime(
                '{:04d}{:02d}{:02d}'.format(year, month, day), '%Y%m%d')
            image_date = image_dt.date().isoformat()

            # Export products
            for product in ini['EXPORT']['products']:
                export_id = ini['EXPORT']['export_id_fmt'] \
                    .replace('_{start}', '') \
                    .replace('_{end}', '') \
                    .format(
                    model=ini['INPUTS']['et_model'].lower(),
                    product=product.lower(),
                    study_area=ini['INPUTS']['study_area_name'],
                    index=scene_id,
                    export=ini['EXPORT']['export_dest'].lower())
                export_id = export_id.replace('-', '')
                logging.debug('  Export ID: {0}'.format(export_id))

                export_path = '{}/{}/{}'.format(
                    ini['EXPORT']['output_ws'], product, export_id + '.tif')

                # Get a Landsat collection with only the target image
                landsat_image = ee.Image(landsat.get_landsat_image(
                    wrs2_tile_list=export_info['wrs2_tiles'],
                    cloud_cover=ini['INPUTS']['cloud_cover'],
                    start_date=image_date))

                # Compute SSEBop ETf for each Landsat scene
                s = ssebop.SSEBop(
                    image=ee.Image(ssebop.landsat_prep(landsat_image)),
                    dt_source=ini['SSEBOP']['dt_source'],
                    elev_source=ini['SSEBOP']['elev_source'],
                    tcorr_source=ini['SSEBOP']['tcorr_source'],
                    tmax_source=ini['SSEBOP']['tmax_source'],
                    elr_flag=ini['SSEBOP']['elr_flag'],
                    tdiff_threshold=ini['SSEBOP']['tdiff_threshold'])
                daily_et_fraction_image = ee.Image(s.compute_etf())

                logging.debug('  Properties: {}'.format(
                    pprint.pformat(daily_et_fraction_image.getInfo()['properties'])))
                # Image date reference ET
                daily_et_reference_image = ee.Image(ssebop.reference_et(
                    start_date=image_date,
                    refet_source=ini['SSEBOP']['refet_source'],
                    refet_type=ini['SSEBOP']['refet_type'],
                    refet_factor=ini['SSEBOP']['refet_factor']).first())

                if irrigation_mask in ['irr', 'inv_irr']:
                    irr_coll = ee.ImageCollection('projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp')
                    coll = irr_coll.filterDate('1987-01-01', '2022-12-31').select('classification')
                    remap = coll.map(lambda img: img.lt(1))
                    irr_mask = remap.sum().gt(4)
                    yr = start[:4]
                    irr = irr_coll.filterDate('{}-01-01'.format(yr), '{}-12-31'.format(yr)).select(
                        'classification').mosaic()

                    if irrigation_mask == 'irr':
                        irr_mask = irr_mask.updateMask(irr.lt(1))
                        logging.debug('    Masking with IrrMapper')
                        daily_et_fraction_image = daily_et_fraction_image.mask(irr_mask)
                    elif irrigation_mask == 'inv_irr':
                        daily_et_fraction_image = daily_et_fraction_image.mask(irr.gt(0))

                # Compute target product
                if product == 'et_actual':
                    output_image = daily_et_fraction_image \
                        .multiply(daily_et_reference_image).multiply(scale).toUint8()
                elif product == 'et_reference':
                    output_image = daily_et_reference_image
                elif product == 'et_fraction':
                    output_image = daily_et_fraction_image.multiply(scale).int()
                elif product in ['count', 'count_mask']:
                    output_image = daily_et_fraction_image.mask()

                else:
                    logging.debug('  Unsupported product {}, skipping'.format(
                        product))
                    continue

                if clip:
                    output_image = output_image.clip(clip.geometry())

                # Build export tasks
                # Export the image to cloud storage

                if debug:
                    point = ee.Geometry.Point([-105.80117, 46.16047])
                    data = output_image.sample(point, 30).getInfo()
                    print(data['features'])

                task = ee.batch.Export.image.toCloudStorage(
                    output_image,
                    description='etf_{}_{}'.format(irrigation_mask, scene_id),
                    bucket='wudr',
                    region=clip.geometry(),
                    crs='EPSG:5070',
                    scale=30,
                    maxPixels=1e13)

                # Try to start the export task a few times
                logging.debug('  Starting export task')
                for i in range(1, 10):
                    try:
                        task.start()
                        break
                    except Exception as e:
                        logging.error(
                            '    Error: {}\n    Retrying ({}/10)'.format(e, i))
                        time.sleep(i ** 2)
                        i += 1


def wrs2_tile_export_generator(study_area_path, wrs2_coll,
                               cell_size=30, output_crs=None, output_osr=None,
                               wrs2_tile_list=[], wrs2_tile_field='WRS2_TILE',
                               snap_x=15, snap_y=15, wrs2_buffer=0,
                               n_max=1000, simplify_buffer=1000):
    """Generate WRS2 tile image metadata for the study area geometry

    Args:
        study_area_path (str): File path of the study area shapefile
        wrs2_coll (str): WRS2 Landsat footprint asset ID.
            (should default to "projects/ssebop-gee/wrs2_descending_custom")
        cell_size (float): Cell size [m].  Defaults to 30.
        output_crs (str): Output CRS (for setting 'crs' parameter in EE calls).
            Defaults to None.
        output_osr (osr.SpatialReference): Output coordinate system.
            Defaults to None.
        wrs2_tile_field (str): WRS2 tile field name in the fusion table
            Defaults to 'WRS2_TILE'
        wrs2_tile_list (list): User defined WRS2 tile subset
        snap_x (float): X snap coordinate [m].  Defaults to 15.
        snap_y (float): Y snap coordinate [m].  Defaults to 15.
        wrs2_buffer (float): WRS2 footprint buffer distance [m].
            Defaults to 10000.
        n_max (int): Maximum number of WRS2 tiles to join to feature.
            Defaults to 1000.
        simplify_buffer (float): Study area buffer/simplify distance [m].
            Defaults to 1000.

    Yields:
        dict: export information
    """
    logging.info('\nReading study area shapefile')
    logging.info('  {}'.format(study_area_path))
    study_area_ds = ogr.Open(study_area_path, 0)
    study_area_lyr = study_area_ds.GetLayer()
    study_area_osr = study_area_lyr.GetSpatialRef()
    study_area_proj = study_area_osr.ExportToWkt()
    # study_area_osr = study_area_osr.ExportToProj4()
    # logging.debug('  Projection: {}'.format(study_area_proj))
    # Convert WKT to EE WKT
    # study_area_crs = re.sub(
    #     '\s+', '', ee.Projection(study_area_proj).wkt().getInfo())
    study_area_crs = str(study_area_proj)
    logging.debug('  Study area projection: {}'.format(study_area_crs))

    # Get the dissolved/unioned geometry of the study area
    output_geom = ogr.Geometry(ogr.wkbMultiPolygon)
    # shape_list = []
    for study_area_ftr in study_area_lyr:
        # Union each feature
        output_geom = output_geom.Union(
            study_area_ftr.GetGeometryRef())
    study_area_ds = None

    # Project the study area geometry to the EPSG:3857
    #   so units will be meters for buffering and simplifying
    temp_crs = 'EPSG:3857'
    temp_osr = osr.SpatialReference()
    temp_osr.ImportFromEPSG(3857)
    output_tx = osr.CoordinateTransformation(study_area_osr, temp_osr)
    output_geom.Transform(output_tx)

    # Buffer/simplify values are assuming the geometry units are in meters
    output_simplify = output_geom.Buffer(simplify_buffer) \
        .SimplifyPreserveTopology(simplify_buffer)

    # Generate an EE feature
    output_ee_geom = ee.Geometry(
        json.loads(output_simplify.ExportToJson()), temp_crs, False)

    # Pre-filter the WRS2 descending collection
    #   with the buffered study area geometry
    # Then buffer the WRS2 descending collection
    if wrs2_buffer:
        wrs2_coll = ee.FeatureCollection(wrs2_coll) \
            .filterBounds(output_ee_geom.buffer(wrs2_buffer, 1)) \
            .map(lambda ftr: ftr.buffer(wrs2_buffer, 1))
    else:
        wrs2_coll = ee.FeatureCollection(wrs2_coll) \
            .filterBounds(output_ee_geom)

    #  Join intersecting geometries
    join_coll = ee.Join.saveAll(matchesKey='scenes').apply(
        ee.FeatureCollection([ee.Feature(output_ee_geom)]), wrs2_coll,
        ee.Filter.intersects(leftField='.geo', rightField='.geo', maxError=10))

    # It is not necessary to map over the join collection
    #   since there is only one study area feature
    output_wrs2_tiles = ee.List(ee.Feature(join_coll.first()).get('scenes'))

    def wrs2_bounds(ftr):
        crs = ee.String('EPSG:').cat(
            ee.Number(ee.Feature(ftr).get('EPSG')).format('%d'))
        extent = ee.Feature(ftr).geometry() \
            .bounds(1, ee.Projection(crs)).coordinates().get(0)
        # extent = ee.Array(extent).transpose().toList()
        # extent = ee.List([
        #   ee.List(extent.get(0)).reduce(ee.Reducer.min()),
        #   ee.List(extent.get(1)).reduce(ee.Reducer.min()),
        #   ee.List(extent.get(0)).reduce(ee.Reducer.max()),
        #   ee.List(extent.get(1)).reduce(ee.Reducer.max())
        # ])
        return ee.Feature(None, {
            'crs': crs,
            'extent': extent,
            'wrs2_tile': ee.Feature(ftr).get(wrs2_tile_field)})

    output_list = output_wrs2_tiles.map(wrs2_bounds).getInfo()

    for output_info in output_list:
        wrs2_tile = output_info['properties']['wrs2_tile']
        if wrs2_tile_list and wrs2_tile not in wrs2_tile_list:
            logging.debug('  WRS2 tile {} not in INI WRS2 tiles, skipping'.format(
                wrs2_tile))
            continue

        # Use output CRS if it was set, otherwise use WRS2 tile CRS
        if output_crs is None:
            wrs2_tile_crs = output_info['properties']['crs']
        else:
            wrs2_tile_crs = output_crs

        output_extent = output_info['properties']['extent']
        output_extent = [
            min([x[0] for x in output_extent]),
            min([x[1] for x in output_extent]),
            max([x[0] for x in output_extent]),
            max([x[1] for x in output_extent])]

        # Adjust extent to the cell size
        adjust_size = 2 * cell_size
        output_extent[0] = math.floor((
                                          output_extent[0] - snap_x) / adjust_size) * adjust_size + snap_x
        output_extent[1] = math.floor((
                                          output_extent[1] - snap_y) / adjust_size) * adjust_size + snap_y
        output_extent[2] = math.ceil((
                                         output_extent[2] - snap_x) / adjust_size) * adjust_size + snap_x
        output_extent[3] = math.ceil((
                                         output_extent[3] - snap_y) / adjust_size) * adjust_size + snap_y

        output_geo = [
            cell_size, 0, output_extent[0], 0, -cell_size, output_extent[3]]

        # output_geom = extent_geom(output_extent)

        output_shape = '{0}x{1}'.format(
            int(abs(output_extent[2] - output_extent[0]) / cell_size),
            int(abs(output_extent[3] - output_extent[1]) / cell_size))

        max_pixels = 2 * reduce(mul, map(int, output_shape.split('x')))

        yield {
            'crs': wrs2_tile_crs,
            'extent': output_extent,
            'geo': '[' + ','.join(map(str, output_geo)) + ']',
            # 'geojson': json.loads(output_geom.ExportToJson()),
            'index': wrs2_tile,
            'maxpixels': max_pixels,
            'wrs2_tiles': [wrs2_tile],
            'shape': output_shape
        }


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export daily ETf images per WRS2 tile',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-i', '--ini', type=utils.arg_valid_file,
        help='Input file', metavar='FILE')
    parser.add_argument(
        '-o', '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '-d', '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = arg_parse()

    logging.basicConfig(level=args.loglevel, format='%(message)s')
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)
    logging.info('\n{}'.format('#' * 80))
    logging.info('{:<20s} {}'.format(
        'Run Time Stamp:', datetime.datetime.now().isoformat(' ')))
    logging.info('{:<20s} {}'.format('Current Directory:', os.getcwd()))
    logging.info('{:<20s} {}'.format(
        'Script:', os.path.basename(sys.argv[0])))

    bad_scenes = []
    err_out = '/home/dgketchum/PycharmProjects/usgs-ssebop-gee/export_tools/missing_tcorr.txt'
    args.ini = '/home/dgketchum/PycharmProjects/usgs-ssebop-gee/export_tools/export_ssebop_wrs2.ini'

    fc = ee.FeatureCollection(ee.Feature(ee.Geometry.Polygon([[-105.85544392193924, 46.105576651626485],
                                                              [-105.70747181500565, 46.105576651626485],
                                                              [-105.70747181500565, 46.222566236544104],
                                                              [-105.85544392193924, 46.222566236544104],
                                                              [-105.85544392193924, 46.105576651626485]]),
                                         {'key': 'Tongue_Ex'}))

    for mask in ['inv_irr', 'irr']:
        for year in list(range(1987, 2023)):
            try:
                start_, end_ = '{}-01-01'.format(year), '{}-12-31'.format(year)
                err = main(ini_path=args.ini, start=start_, end=end_, scale=1000,
                           irrigation_mask=mask, clip=fc, debug=False)
            except ee.ee_exception.EEException as e:
                print(year, e)
        print(bad_scenes)
