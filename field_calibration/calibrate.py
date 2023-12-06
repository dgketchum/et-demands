import pycup as cp
import numpy as np
import pandas as pd

from fieldET import obs_field_cycle
from fieldET import obs_crop_et_data
from fieldET import obs_et_cell

dct = {'rew': [2.0, 12.0],
       'tew': [6.0, 29.0],
       'aw': [100.0, 1000.0]}


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
    predicted = result[['et_act']]
    predicted.dropna(inplace=True)
    idx = [i for i in predicted.index if i in obs.index]
    target, pred = obs.loc[idx].values, predicted.loc[idx].values
    fitness = cp.evaluation_metrics.MSE(target, pred)
    predicted = np.array(predicted).reshape(1, -1)

    return fitness, predictedinitial


def objective_function(X):
    model = X * data
    dct.update({'param': X})
    fitness = cp.evaluation_metrics.OneMinusNSE(obj, model)
    model = np.array(model).reshape(1, -1)
    return fitness, model


if __name__ == '__main__':
    # basic example
    # lb = np.array([0.5])
    # ub = np.array([3.5])
    # hs, hr, hf = cp.GLUE.run(100, 1, lb=lb, ub=ub, fun=objective_function)

    obs_file = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/landsat/field_daily/1778_daily.csv'
    data = pd.read_csv(obs_file, index_col=0, parse_dates=True, infer_datetime_format=True)
    data = data[['eta_r_mm']]
    data.dropna(inplace=True)

    ini = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/tongue_example_cet_obs.ini'
    field_id = '1786'

    params = [(k, v[0], v[1]) for k, v in dct.items()]
    lb = np.array([i[1] for i in params])
    ub = np.array([i[2] for i in params])

    hs, hr, hf = cp.GLUE.run(5, 3, lb=lb, ub=ub, fun=calibrate_unirrigated,
                             args=(field_id, ini, data))
    a = 1
# ========================= EOF ====================================================================
