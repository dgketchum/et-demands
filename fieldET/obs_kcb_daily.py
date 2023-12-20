"""kcb_daily.py
Defines kcb_daily function
Called by crop_cycle.py

"""

import datetime
import logging



def kcb_daily(data, et_cell, crop, foo, foo_day,
              ndvi_coeff, debug_flag=False):
    """Compute basal ET

    Parameters
    ---------
        data :

        et_cell :
        crop :
        foo :
        foo_day :

        debug_flag : boolean
            True : write debug level comments to debug.txt
            False

    Returns
    -------
    None

    Notes
    -----

    """

    # Set MAD to MADmid universally atstart.
    # Value will be changed later.  R.Allen 12/14/2011
    foo.mad = foo.mad_mid

    gs_start, gs_end = datetime.datetime(foo_day.year, 4, 1), datetime.datetime(foo_day.year, 10, 31)
    gs_start_doy, gs_end_doy = int(gs_start.strftime('%j')), int(gs_end.strftime('%j'))

    # if gs_start_doy < foo_day.doy < gs_end_doy:
    if foo_day.year in et_cell.fallow_years or data.field_type == 'unirrigated':
        kc_src = '{}_NO_IRR'.format(data.kc_proxy)
    else:
        kc_src = '{}_IRR'.format(data.kc_proxy)
    dt_string = '{}-{:02d}-{:02d}'.format(foo_day.year, foo_day.month, foo_day.day)
    foo.kc_bas = et_cell.crop_coeffs[1].data.loc[dt_string, kc_src]

    # Save kcb value for use tomorrow in case curve needs to be extended until frost
    # Save kc_bas_prev prior to CO2 adjustment to avoid double correction
    foo.kc_bas_prev = foo.kc_bas

    # Water has only 'kcb'

    foo.kc_act = foo.kc_bas
    foo.kc_pot = foo.kc_bas

    # ETr changed to ETref 12/26/2007
    foo.etc_act = foo.kc_act * foo_day.etref
    foo.etc_pot = foo.kc_pot * foo_day.etref
    foo.etc_bas = foo.kc_bas * foo_day.etref

    # Save kcb value for use tomorrow in case curve needs to be extended until frost
    # Save kc_bas_prev prior to CO2 adjustment to avoid double correction

    # dgketchum mod to 'turn on' root growth
    if foo_day.doy > gs_start_doy and 0.10 <= foo.kc_bas:
        foo.grow_root = True
    elif foo_day.doy < gs_start_doy or foo_day.doy > gs_end_doy:
        foo.grow_root = False

    foo.kc_bas_prev = foo.kc_bas

    foo.height = max(foo.height, 0.05)

    # RHmin and U2 are computed in ETCell.set_weather_data()
    # Allen 3/26/08

    if data.refet['type'] == 'eto':
        # ******'12/26/07
        foo.kc_bas = (
            foo.kc_bas + (0.04 * (foo_day.u2 - 2) - 0.004 * (foo_day.rh_min - 45)) *
            (foo.height / 3) ** 0.3)
        logging.debug(
            'kcb_daily(): kcb %.6f  u2 %.6f  rh_min %.6f  height %.6f' %
            (foo.kc_bas, foo_day.u2, foo_day.rh_min, foo.height))

    # ETr basis, therefore, no adjustment to kcb

    elif data.refet['type'] == 'etr':
        pass
