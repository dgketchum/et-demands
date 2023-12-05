import os

import pandas as pd
from sklearn.linear_model import LinearRegression


def estimate_slope_intercept(time_series_d, irr_file, partition='irr'):

    target = None
    combined_df = pd.DataFrame()

    irr = pd.read_csv(irr_file, index_col=0)
    irr.drop(columns=['LAT', 'LON'], inplace=True)
    cols = [int(c.split('_')[1]) for c in list(irr.columns)]
    irr.columns = cols
    irr = irr.T.to_dict()

    file_list = [os.path.join(time_series_d, f) for f in os.listdir(time_series_d) if f.endswith('.csv')]

    for file_path in file_list:
        sid = int(os.path.basename(file_path).replace('_daily.csv', ''))
        df = pd.read_csv(file_path, infer_datetime_format=True, index_col=0, parse_dates=True)
        df['etf'] = df['eta_r_mm'] / df['etr_mm']
        df = df[['etf', 'NDVI_NO_IRR', 'NDVI_IRR']]

        if partition == 'unirr':
            target = 'NDVI_NO_IRR'
            years = [y for y, f in irr[sid].items() if f < 0.9]
        else:
            years = [y for y, f in irr[sid].items() if f > 0.9]
            target = 'NDVI_IRR'

        idx = [i for i in df.index if i.year in years and 3 < i.month < 11]
        df = df.loc[idx]
        combined_df = combined_df.append(df, ignore_index=True)

    combined_df = combined_df.dropna(subset=['NDVI_NO_IRR', 'NDVI_IRR', 'etf'])

    print('NDVI/Kc: {}'.format(regress(combined_df[target].values, combined_df['etf'].values)))


def regress(x, y):
    x, y = x.reshape(-1, 1), y.reshape(-1, 1)
    model = LinearRegression().fit(x, y)
    slope = model.coef_[0].item()
    intercept = model.intercept_.item()
    return slope, intercept


if __name__ == '__main__':
    d = '/home/dgketchum/PycharmProjects/et-demands'

    irr_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'tongue_sample_irr.csv')
    fields_shp = os.path.join(d, 'examples', 'tongue', 'gis', 'tongue_fields_sample.shp')
    dst_dir_ = os.path.join(d, 'examples', 'tongue', 'landsat', 'ndvi', 'field_daily')

    estimate_slope_intercept(dst_dir_, irr_, partition='irr')
# ========================= EOF ====================================================================
