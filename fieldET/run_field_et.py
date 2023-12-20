import os
import time
import logging

import numpy as np
import pandas as pd

from fieldET import obs_field_cycle
from fieldET import obs_crop_et_data
from fieldET import obs_et_cell


def run_fields(ini_path, debug_flag=False, field_type='irrigated', target_field='2100'):

    data = obs_crop_et_data.ObsFieldETData(field_type=field_type)
    data.read_cet_ini(ini_path, debug_flag)
    data.set_crop_params()
    cells = obs_et_cell.ObsETCellData()
    cells.set_cell_properties(data)
    cells.set_cell_cuttings_irrigation(data)
    cells.set_field_crops(data)
    cells.set_static_crop_params(data.crop_params)

    cell_count = 0
    for cell_id, cell in sorted(cells.et_cells_dict.items()):

        if cell_id != target_field:
            continue

        cell_count += 1

        cell.set_input_timeseries(cell_count, data, cells)
        start_time = time.time()
        df = obs_field_cycle.field_day_loop(data, cell, debug_flag=debug_flag, return_df=True)
        pred = df['et_act'].values

        np.savetxt(os.path.join(d, 'pest', 'eta.np'), pred)

        obs = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/obs.np'
        obs = np.loadtxt(obs)
        cols = ['et_obs'] + list(df.columns)
        df['et_obs'] = obs
        df = df[cols]

        comp = pd.DataFrame(data=np.vstack([obs, pred]).T, columns=['obs', 'pred'], index=df.index)
        comp['eq'] = comp['obs'] == comp['pred']

        rmse = np.sqrt(((pred - obs)**2).mean())
        end_time = time.time()
        print('Execution time: {:.2f}'.format(end_time - start_time))
        print('Mean Obs: {:.2f}, Mean Pred: {:.2f}'.format(obs.mean(), pred.mean()))
        print('RMSE: {:.4f}\n\n\n\n'.format(rmse))
        # df = df.loc['2003-01-01': '2003-12-31']
        pass


if __name__ == '__main__':

    d = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/'
    ini = os.path.join(d, 'tongue_example_cet_obs.ini')
    run_fields(ini_path=ini, debug_flag=False, field_type='unirrigated')
