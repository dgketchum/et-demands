import os
import json

import pandas as pd


def write_field_properties(irr, cdl, js):

    irr = pd.read_csv(irr, index_col='FID')
    irr.drop(columns=['LAT', 'LON'], inplace=True)
    dct = irr.T.to_dict()
    dct = {k: {'irr': {int(kk.split('_')[1]): vv for kk, vv in v.items()}} for k, v in dct.items()}
    cdl = pd.read_csv(cdl, index_col='FID')
    cdl.drop(columns=['LAT', 'LON'], inplace=True)
    cdl = cdl.T.to_dict()
    [dct[k].update({'cdl': {int(kk.split('_')[1]): int(vv) for kk, vv in cdl[k].items()}}) for k in dct.keys()]
    with open(js, 'w') as fp:
        json.dump(dct, fp, indent=4)


if __name__ == '__main__':
    d = '/media/research/IrrigationGIS/et-demands'
    project = 'flynn'
    project_ws = os.path.join(d, 'examples', project)

    irr_ = os.path.join(project_ws, 'properties', '{}_sample_irr.csv'.format(project))
    cdl_ = os.path.join(project_ws, 'properties', '{}_sample_cdl.csv'.format(project))
    jsn = os.path.join(project_ws, 'properties', '{}_sample_props.json'.format(project))

    write_field_properties(irr_, cdl_, jsn)

# ========================= EOF ====================================================================
