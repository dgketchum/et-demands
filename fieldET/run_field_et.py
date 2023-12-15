import os
import time
import logging

import pandas as pd

from fieldET import obs_field_cycle
from fieldET import obs_crop_et_data
from fieldET import obs_et_cell


def run_fields(ini_path, debug_flag=False):

    data = obs_crop_et_data.ObsFieldETData()
    data.read_cet_ini(ini_path, debug_flag)
    data.set_crop_params()
    cells = obs_et_cell.ObsETCellData()
    cells.set_cell_properties(data)
    cells.set_cell_cuttings_irrigation(data)
    cells.set_field_crops(data)
    cells.set_static_crop_params(data.crop_params)

    cell_count = 0
    for cell_id, cell in sorted(cells.et_cells_dict.items()):

        if cell_id != '1786':
            continue

        cell_count += 1

        cell.set_input_timeseries(cell_count, data, cells)
        start_time = time.time()
        obs_field_cycle.field_day_loop(data, cell, debug_flag=debug_flag)
        end_time = time.time()
        # print('Execution time: {:.2f}'.format(end_time - start_time))


if __name__ == '__main__':

    d = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/'
    field_id = '1786'
    ini = os.path.join(d, 'tongue_example_cet_obs.ini')

    # logging.basicConfig(level=logging.ERROR)
    # overwrite = True
    # run_fields(ini_path=ini, debug_flag=False)
    # model_out = os.path.join(d, 'obs_daily_stats/{}_crop_01.csv'.format(field_id))
    # data = pd.read_csv(model_out, index_col=0, parse_dates=True, header=1)
    # print(data[['ETact']].values.mean())

    mult = os.path.join(d, 'pest', 'mult')

    run_fields(ini_path=ini, debug_flag=False)
