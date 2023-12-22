import json

import geopandas as gpd
import pandas as pd


class ProjectFields:
    """A Container for historical and static field information

    This should include some initial estimate of soil properties and historical
    estimate of irrigated status and crop type.

    """
    def __init__(self):
        super().__init__()
        self.fields_dict = None

    def initialize_field_data(self, config):

        self.fields_dict = {}

        df = gpd.read_file(config.fields_path)
        df.index = df[config.field_index]

        for fid, row in df.iterrows():
            field = FieldData()

            field.field_id = str(fid)
            field.lat = row['LAT']
            field.lon = row['LON']
            field.geometry = row['geometry']

            self.fields_dict[field.field_id] = field

            field.set_input_timeseries(config)
            field.set_field_properties(config)


class FieldData:
    def __init__(self):
        super().__init__()
        self.props = None
        self.refet_df = None
        self.crop_coeffs = None
        self.field_id = None
        self.field_id = None
        self.lat = None
        self.lon = None
        self.geometry = None
        self.input = None

    def set_input_timeseries(self, config):
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

        f = config.input_timeseries.format(self.field_id)
        df = pd.read_csv(f, parse_dates=True, index_col=0)
        self.input = df

    def set_field_properties(self, config):
        f = config.field_properties
        with open(f, 'r') as fp:
            dct = json.load(fp)
        self.props = dct[str(self.field_id)]


if __name__ == '__main__':
    pass
