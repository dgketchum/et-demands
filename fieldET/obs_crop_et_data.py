"""crop_et_data.py
Defines CropETData class
Functions in class to read INI file, refet data, met data, ...
Called by mod_crop_et.py

"""

import configparser
import logging
import os
import pandas as pd


class ProjectConfig:
    """Crop et data container

    Attributes
    ----------

    """

    def __init__(self, field_type='irrigated'):
        super().__init__()
        self.cover_proxy = None
        self.field_cuttings = None
        self.refet_type = None
        self.soils = None
        self.field_index = None
        self.calibration = None
        self.calibration_folder = None
        self.calibrated_parameters = None
        self.calibration_files = None
        self.parameter_values = None
        self.field_type = field_type
        self.kc_proxy = None
        self.project_ws = None
        self.ts_quantity = None
        self.start_dt = None
        self.end_dt = None
        self.sensing_folder = None
        self.cell_crops_path = None
        self.elev_units = None
        self.field_properties = None
        self.input_timeseries = None
        self.fields_path = None

    def __str__(self):
        """ """
        return '<Cropet_data>'

    def read_cet_ini(self, ini_path, debug_flag=False):
        """Read and parse INI file
        Parameters
        ---------
        ini_path : str
            absolute file path to INI file
        debug_flag : boolean
            True : write debug level comments to debug.txt
            False

        Returns
        -------

        Notes
        -----

        """
        logging.info('  INI: {}'.format(os.path.basename(ini_path)))

        # Check that INI file can be read
        config = configparser.RawConfigParser()
        config.read_file(open(ini_path))

        crop_et_sec = 'CROP_ET'
        calib_sec = 'CALIBRATION'

        self.kc_proxy = config.get(crop_et_sec, 'kc_proxy')
        self.cover_proxy = config.get(crop_et_sec, 'cover_proxy')

        self.project_ws = config.get(crop_et_sec, 'project_folder')
        self.field_index = 'FID'

        assert os.path.isdir(self.project_ws)

        self.ts_quantity = int(1)

        sdt = config.get(crop_et_sec, 'start_date')
        self.start_dt = pd.to_datetime(sdt)
        edt = config.get(crop_et_sec, 'end_date')
        self.end_dt = pd.to_datetime(edt)

        # elevation units
        self.elev_units = config.get(crop_et_sec, 'elev_units')
        assert self.elev_units == 'm'

        self.refet_type = config.get(crop_et_sec, 'refet_type')

        # et cells properties
        self.soils = config.get(crop_et_sec, 'soils')

        self.fields_path = config.get(crop_et_sec, 'fields_path')
        self.field_properties = config.get(crop_et_sec, 'field_properties')
        self.input_timeseries = config.get(crop_et_sec, 'input_timeseries')
        self.irrigation_data = config.get(crop_et_sec, 'irrigation_data')

        self.calibration = bool(config.get(calib_sec, 'calibrate_flag'))

        if self.calibration:
            cf = config.get(calib_sec, 'calibration_folder')
            self.calibration_folder = cf
            self.calibrated_parameters = config.get(calib_sec, 'calibrated_parameters').split(',')
            _files = sorted([os.path.join(cf, f) for f in os.listdir(cf)])
            self.calibration_files = {k: v for k, v in zip(self.calibrated_parameters, _files)}
