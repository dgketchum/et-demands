import os
import pandas as pd
import numpy as np

import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

project = 'flynn'
_in = '/media/research/IrrigationGIS/et-demands/examples/{}'.format(project)
_out = '/home/dgketchum/PycharmProjects/et-demands/examples/{}'.format(project)
field_id = '3'


def preproc():
    obs_file = os.path.join(_in, 'input_timeseries/{}_daily.csv'.format(field_id))
    data = pd.read_csv(obs_file, index_col=0, parse_dates=True)
    data.index = list(range(data.shape[0]))
    data['eta'] = data['eto_mm'] * data['etf_inv_irr']
    data = data[['eta']]
    data.dropna(inplace=True)
    print('preproc mean: {}'.format(data.values.mean()))
    _file = os.path.join(_out, 'eta.np')
    np.savetxt(_file, data.values)
    _file = os.path.join(_out, 'obs.np')
    np.savetxt(_file, data.values)
    print('Writing Obs to {}'.format(_file))


preproc()
