"""et_cell.py
Defines ETCellData class
Defines crop_cycle_mp, crop_cycle, crop_day_loop_mp, crop_day_loop,
    write_crop_output
Called by mod_crop_et.py

"""

import logging
import copy
import os
import json
import sys

import datetime

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             '../lib')))
from fieldET import util

from fieldET.et_cell import ETCellData, ETCell
from fieldET.obs_remote_sensing import RemoteSensingData

mpdToMps = 3.2808399 * 5280 / 86400


class ObsETCellData(ETCellData):
    def __init__(self):
        super().__init__()

    def set_cell_cuttings_irrigation(self, data):
        """Extract mean cutting data from specified file

        Parameters
        ---------
        data : dict
            configuration data from INI file

        Returns
        -------
        None

        Notes
        -----

        """

        with open(data.cell_cuttings_path, 'r') as fp:
            dct = json.load(fp)

        for cell_id, row in dct.items():
            cell = self.et_cells_dict[cell_id]
            cell.dairy_cuttings = None
            cell.beef_cuttings = None

            cell.irrigation_data = {}

            for k, v in row.items():
                if k == 'fallow_years':
                    cell.fallow_years = v
                elif k == 'average_cuttings':
                    pass
                else:
                    cell.irrigation_data[int(k)] = v

    def set_cell_properties(self, data):
        """Extract ET cells properties data from specified file

        Parameters
        ---------
        data : dict
            configuration data from INI file

        Returns
        -------
        None

        Notes
        -----
        This function builds ETCell objects and must be run first

        """

        logging.info('\nReading ET Cells properties data from\n' +
                     data.cell_properties_path)
        try:

            df = pd.read_csv(data.cell_properties_path)
            uc_columns = list(df.columns)
            columns = [x.lower() for x in uc_columns]

            for rc, row in df.iterrows():
                cell = ObsETCell()
                if not (cell.read_cell_properties_from_row(row.tolist(), columns,
                                                           data.elev_units)):
                    sys.exit()
                self.et_cells_dict[cell.cell_id] = cell
        except:
            logging.error('Unable to read ET Cells Properties from ' +
                          data.cell_properties_path)
            logging.error('\nERROR: ' + str(sys.exc_info()[0]) + 'occurred\n')
            sys.exit()

    def set_dynamic_crop_coeffs(self, crop_coeffs):
        """set static crop coefficients

        Parameters
        ---------
        crop_coeffs :


        Returns
        -------
        None

        Notes
        -----

        """

        logging.info('Setting static crop coefficients')
        for cell_id in sorted(self.et_cells_dict.keys()):
            cell = self.et_cells_dict[cell_id]
            cell.crop_coeffs = copy.deepcopy(crop_coeffs)

    def set_static_crop_params(self, crop_params):
        """set static crop parameters

        Parameters
        ---------
        crop_params :


        Returns
        -------
        None

        Notes
        -----

        """
        logging.info('\nSetting static crop parameters')

        # copy crop_params
        for cell_id in sorted(self.et_cells_dict.keys()):
            cell = self.et_cells_dict[cell_id]
            cell.crop_params = copy.deepcopy(crop_params)

    def set_field_crops(self, data):
        """Read crop crop flags using specified file type

        Parameters
        ---------
        data : dict
            configuration data from INI file

        Returns
        -------
        None

        Notes
        -----

        """

        self.read_field_crops(data)

    def read_field_crops(self, data):
        """Extract et cell crop data from text file

        Parameters
        ---------
        data : dict
            configuration data from INI file

        Returns
        -------
        None

        Notes
        -----

        """

        logging.info('\nReading cell crop flags from\n' + data.cell_crops_path)

        with open(data.cell_crops_path, 'r') as fp:
            dct = json.load(fp)

        for i, row in enumerate(dct.items()):
            cell_id = row[0]
            cell = self.et_cells_dict[cell_id]
            # cell.irrigation_flag = int(data[3])
            # cell.crop_flags = dict(zip(row[1]['etd'], data[4:].astype(bool)))
            # cell.ncrops = len(self.crop_flags)
            # cell.crop_names = crop_names
            cell.crop_numbers = row[1]['etd']

            # cell.crop_num_list = sorted(
            #     [k for k, v in cell.crop_flags.items() if v])
            self.crop_num_list.extend(row[1]['etd'])

        # Update list of active crop numbers in all cells

        self.crop_num_list = sorted(list(set(self.crop_num_list)))


class ObsETCell(ETCell):
    def __init__(self):
        super().__init__()
        self.refet_df = None
        self.crop_coeffs = None

    def read_cell_properties_from_row(self, row, columns, elev_units='feet'):
        """ Parse row of data from ET Cells properties file

        Parameters
        ---------
        row : list
            one row of ET Cells Properties
        start_column : int
            first zero based row column

        Returns
        -------
        : boolean
            True
            False

        """
        # ET Cell id is cell id for crop and area et computations
        # Ref ET MET ID is met node id, aka ref et node id of met and ref et row

        try:
            self.cell_id = str(row[columns.index('fid')])

            self.cell_name = str(row[columns.index('fid')])

            self.refet_id = int(row[columns.index('gfid')])

            self.latitude = float(row[columns.index('lat')])

            self.longitude = float(row[columns.index('lon')])

            self.elevation = float(row[columns.index('elev')])
            if elev_units == 'feet' or elev_units == 'ft':
                self.elevation *= 0.3048

            self.air_pressure = util.pair_from_elev(0.3048 * self.elevation)

            self.permeability = float(row[columns.index('aridity_rating')])

            self.stn_whc = float(row[columns.index('awc_in_ft')])

            self.stn_soildepth = float(row[columns.index('soil_depth')])

            self.stn_hydrogroup_str = str(row[columns.index('hydgrp')])

            self.stn_hydrogroup = int(row[columns.index('hydgrp_num')])

            self.aridity_rating = float(row[columns.index('permeability')])

            return True

        except Exception as e:
            print(e)
            print(logging.error('Unable to read parameter'))
            return False

    def set_input_timeseries(self, cell_count, data, cells):
        """Wrapper for setting all refet and met data

        Parameters
        ---------
        cell_count : int
            count of et cell being processed
        data : dict
            configuration data from INI file
        cells : dict
            eT cells data

        Returns
        -------
        : boolean
            True
            False

        """

        self.set_refet_data(data, cells)
        self.set_weather_data(data, cells)
        self.process_climate(data)
        self.set_field_remote_sensing_data(data)
        return True

    def set_field_remote_sensing_data(self, data):
        """Read daily crop coefficient data from NDVI time series"""

        dct = {'curve_type_no': '5'}
        rs_obj = RemoteSensingData(**dct)
        _csv = os.path.join(data.crop_coefs_path, '{}_daily.csv'.format(self.cell_id))
        coeff_df = pd.read_csv(_csv, index_col='date', infer_datetime_format=True, parse_dates=True)
        rs_obj.data = coeff_df[['eta_r_mm', 'eta_o_mm', 'NDVI_NO_IRR', 'NDVI_IRR', 'ETF_NO_IRR', 'ETF_IRR']]
        self.crop_coeffs = {1: rs_obj}

    def set_refet_data(self, data, cells):
        """Read daily crop coefficient data from NDVI time series"""

        _csv = os.path.join(data.crop_coefs_path, '{}_daily.csv'.format(self.cell_id))
        refet_df = pd.read_csv(_csv, index_col='date', infer_datetime_format=True, parse_dates=True)
        refet_df = refet_df[['etr_mm', 'eto_mm', 'etr_mm_uncorr', 'eto_mm_uncorr']]
        if data.refet['type'] == 'eto':
            refet_df['etref'] = refet_df['eto_mm']
        else:
            refet_df['etref'] = refet_df['etr_mm']
        refet_df['doy'] = [i.dayofyear for i in refet_df.index]
        self.refet_df = refet_df


if __name__ == '__main__':
    pass
