import os
import time

import numpy as np
import pandas as pd

from fieldET import obs_crop_et_data
from fieldET import obs_et_cell
from fieldET import obs_field_cycle


def run_fields(ini_path, flux_obs, debug_flag=False, field_type='irrigated', target_field='1178'):
    config = obs_crop_et_data.ProjectConfig(field_type=field_type)
    config.read_cet_ini(ini_path, debug_flag)

    fields = obs_et_cell.ProjectFields()
    fields.initialize_field_data(config, target=target_field)

    for fid, field in sorted(fields.fields_dict.items()):

        if fid != target_field:
            continue

        start_time = time.time()
        df = obs_field_cycle.field_day_loop(config, field, debug_flag=debug_flag)
        pred = df['et_act'].values

        np.savetxt(os.path.join(d, 'pest', 'eta.np'), pred)

        obs = pd.read_csv(flux_obs, index_col=0, parse_dates=True)
        cols = ['et_flux'] + ['et_ssebop'] + list(df.columns)
        df['et_flux'] = obs['ET']
        df['et_ssebop'] = field.input['etf_inv_irr'] * field.input['etr_mm']
        df = df[cols]

        comp = df.loc[df[df['capture'] == 1.0].index]
        et_act, et_ssebop = comp['et_act'], comp['et_ssebop']
        rmse = np.sqrt(((et_act - et_ssebop) ** 2).mean())
        end_time = time.time()
        print('Execution time: {:.2f} seconds'.format(end_time - start_time))
        print('Capture Dates; Mean SSEBop: {:.2f}, SWB Pred: {:.2f}'.format(et_ssebop.mean(), et_act.mean()))
        print('SWB ET/SSEBop RMSE: {:.4f}\n\n\n\n'.format(rmse))

        comp = df[~pd.isna(df['et_flux']) == 1].copy()
        et_flux, et_ssebop = comp['et_flux'], comp['et_ssebop']
        rmse = np.sqrt(((et_flux - et_ssebop) ** 2).mean())
        print('Flux Dates; Mean Flux: {:.2f}, Mean SSEBop: {:.2f}'.format(et_flux.mean(), et_ssebop.mean()))
        print('RMSE Flux/SSEBop Dates: {:.4f}\n\n\n\n'.format(rmse))

        comp = df[~pd.isna(df['et_flux']) == 1].copy()
        et_act, et_flux = comp['et_act'], comp['et_flux']
        rmse = np.sqrt(((et_act - et_flux) ** 2).mean())
        print('Flux Dates; Mean Flux: {:.2f}, Mean SWB: {:.2f}'.format(et_flux.mean(), et_act.mean()))
        print('RMSE Flux/SWB Dates: {:.4f}\n\n\n\n'.format(rmse))
        pass


if __name__ == '__main__':
    project = 'flux'
    target = 'US-FPe'
    field_type = 'unirrigated'
    d = '/home/dgketchum/PycharmProjects/et-demands/examples/{}'.format(project)
    ini = os.path.join(d, '{}_example_cet_obs.ini'.format(project))

    flux_obs_ = os.path.join('/media/research/IrrigationGIS/climate/flux_ET_dataset/'
                             'daily_data_files/{}_daily_data.csv'.format(target))

    run_fields(ini_path=ini, flux_obs=flux_obs_, debug_flag=False, field_type=field_type,
               target_field=target)
