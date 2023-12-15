import os
import sys
import subprocess
import multiprocessing as mp

import pandas as pd
import numpy as np

import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

d = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/'
field_id = '1786'


def activate_conda_environment(environment_name):
    try:
        activate_cmd = f"conda run -n {environment_name} /bin/bash"
        subprocess.run(activate_cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Conda environment '{environment_name}' activated.")
    except subprocess.CalledProcessError as e:
        print(f"Error activating Conda environment '{environment_name}': {e}")
        sys.exit(1)


activate_conda_environment('mihm')


def run():
    p = '/home/dgketchum/PycharmProjects/et-demands/fieldET/run_field_et.py'
    os.system('python' + ' {}'.format(p))
    os.chdir("..")


def postproc():
    # print('pest custom script')
    model_out = os.path.join(d, 'obs_daily_stats/{}_crop_01.csv'.format(field_id))
    data = pd.read_csv(model_out, index_col=0, parse_dates=True, header=1)
    data = data[['ETact']]
    data.columns = ['eta']
    np.savetxt(os.path.join(d, 'pest', 'eta.np'), data.values)


if __name__ == '__main__':
    mp.freeze_support()
    run()
    postproc()
