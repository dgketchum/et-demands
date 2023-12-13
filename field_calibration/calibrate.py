import os

import pycup as cp
import numpy as np
import pandas as pd

from fieldET import obs_field_cycle
from fieldET import obs_crop_et_data
from fieldET import obs_et_cell

dct = {'aw': [100.0, 1000.0]}
       # 'rew': [2.0, 12.0],
       # 'tew': [6.0, 29.0], }


def intitialize_field(ini_path, fid, field_type='irrigated'):
    metadata = obs_crop_et_data.ObsFieldETData(field_type=field_type)
    metadata.read_cet_ini(ini_path)
    metadata.set_crop_params()
    cells = obs_et_cell.ObsETCellData()
    cells.set_cell_properties(metadata)
    cells.set_cell_cuttings_irrigation(metadata)
    cells.set_field_crops(metadata)
    cells.set_static_crop_params(metadata.crop_params)
    field = cells.et_cells_dict[fid]

    cell_ct = 1
    field.set_input_timeseries(cell_ct, metadata, cells)

    return metadata, field


def calibrate_unirrigated(beta, fid, ini, obs):
    coeffs = {p: b for p, b in zip(dct.keys(), beta)}
    meta, field = intitialize_field(ini, fid, field_type='unirrigated')
    result = obs_field_cycle.field_day_loop(meta, field, return_df=True, **coeffs)
    predicted = result[['et_act']].copy()
    predicted.dropna(inplace=True)
    idx = [i for i in predicted.index if i in obs.index]
    target, pred = obs.loc[idx].values, predicted.loc[idx].values
    fitness = cp.evaluation_metrics.MSE(target, pred)
    predicted = np.array(predicted).reshape(1, -1)
    return fitness, predicted


if __name__ == '__main__':
    d = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/'
    # basic example
    # lb = np.array([0.5])
    # ub = np.array([3.5])
    # hs, hr, hf = cp.GLUE.run(100, 1, lb=lb, ub=ub, fun=objective_function)

    field_id = '1786'

    obs_file = os.path.join(d, 'landsat/field_daily/{}_daily.csv'.format(field_id))
    data = pd.read_csv(obs_file, index_col=0, parse_dates=True, infer_datetime_format=True)
    data['eta'] = data['etr_mm'] * data['ETF_NO_IRR']
    data = data[['eta']]
    data[data.values <= 0] = np.nan
    data.dropna(inplace=True)
    idx = [i for i in data.index if i.month in list(range(5, 11)) and i.year == 2021]

    ini = os.path.join(d, 'tongue_example_cet_obs.ini')

    params = [(k, v[0], v[1]) for k, v in dct.items()]
    lb = np.array([i[1] for i in params])
    ub = np.array([i[2] for i in params])

    hs, hr, hf = cp.GLUE.run(500, 1, lb=lb, ub=ub, fun=calibrate_unirrigated,
                               args=(field_id, ini, data))
    a = 1
# ========================= EOF ====================================================================
