import os
import json
import pycup as cp
import numpy as np
import pandas as pd

from sklearn.metrics import mean_squared_error

f = '/home/dgketchum/PycharmProjects/et-demands/examples/tongue/landsat/field_daily/1778_daily.csv'
data = pd.read_csv(f, index_col=0)
mask = [i for i, a in data['NDVI_IRR'].items() if not np.isnan(a)]
obj = data.loc[mask, 'ETF_IRR'].values
d = data.loc[mask, 'NDVI_IRR'].values

dct = {'param': [0.0]}


def objective_function(beta):
    model = beta * d
    dct.update({'param': beta})
    fitness = cp.evaluation_metrics.MSE(obj, model)
    model = np.array(model).reshape(1, -1)
    return fitness, model


if __name__ == '__main__':
    lb = np.array([0.5])
    ub = np.array([3.5])
    hs, hr, hf = cp.GLUE.run(100, 1, lb=lb, ub=ub, fun=objective_function)
    a = 1
# ========================= EOF ====================================================================
