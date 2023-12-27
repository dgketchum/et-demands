"""initialize_crop_cycle.py
Defines InitializeCropCycle class
Called by crop_cycle.py
"""

import numpy as np

de_initial = 10.0


class InitializeObsCropCycle:
    def __init__(self):
        """Initialize for crops cycle"""
        self.cover_proxy = None
        self.crop_df = None
        self.ad = 0.
        self.aw = 0
        self.aw3 = 0.
        self.cn2 = 0.
        self.cgdd = 0.
        self.cgdd_penalty = 0.
        self.cum_evap = 0.
        self.cum_evap_prev = 0.
        self.depl_ze = 0.
        self.depl_zep = 0.
        self.dperc = 0.
        self.dperc_ze = 0.
        self.density = 0.
        self.depl_surface = 0.
        self.depl_root = 0.
        self.etc_act = 0.
        self.etc_pot = 0.
        self.etc_bas = 0.
        self.etref_30 = 0.  # thirty day mean ETref  ' added 12/2007
        self.fc = 0.
        self.fw = 0.
        self.fw_spec = 0.
        self.fw_std = 0.
        self.fw_irr = 0.
        self.gdd = 0.0
        self.gdd_penalty = 0.
        self.height_min = 0.
        self.height_max = 0.
        self.height = 0
        self.irr_auto = 0.
        self.irr_sim = 0.
        self.kc_act = 0.
        self.kc_pot = 0
        self.kc_max = 0.
        self.kc_min = 0.
        self.kc_bas = 0.
        self.kc_bas_mid = 0.
        self.kc_bas_prev = 0.
        self.ke = 0.
        self.ke_irr = 0
        self.ke_ppt = 0.
        self.kr2 = 0.
        self.ks = 0.
        self.kt_reducer = 1.
        self.mad = 0.
        self.mad_ini = 0.
        self.mad_mid = 0.
        self.n_cgdd = 0.
        self.n_pl_ec = 0.
        self.niwr = 0.
        self.p_rz = 0.
        self.p_eft = 0.
        self.ppt_inf = 0.
        self.ppt_inf_prev = 0.
        self.rew = 0.
        self.tew = 0.
        self.tew2 = 0.
        self.tew3 = 0.
        self.s = 0.
        self.s1 = 0.
        self.s2 = 0.
        self.s3 = 0.
        self.s4 = 0.
        self.sro = 0.
        self.zr_min = 0.
        self.zr_max = 0.
        self.z = 0.

        # TODO add invoke stess as a tunable parameter
        self.invoke_stress = 0.9

        # CGM - I don't remember why these are grouped separately
        # Maybe because they are "flags"

        self.doy_start_cycle = 0
        self.cutting = 0
        self.cycle = 1
        self.real_start = False
        self.obs_flag = True
        self.irr_flag = False
        self.in_season = False  # false if outside season, true if inside
        self.dormant_setup_flag = False
        self.crop_setup_flag = True  # flag to setup crop parameter information

        # dgketchum hacks to remove crop-type dependence
        self.height_initial = 0.1
        self.height_max = 2.0
        self.height_min = 0.1
        self.height = self.height_initial

        # TP - Looks like its value comes from compute_crop_et(),
        # but needed for setup_dormant() below...

        self.totwatin_ze = 0.

        # CGM - These are not pre-initialized in the 32-bit VB code

        self.cgdd_at_planting = 0.
        self.wt_irr = 0.

        # CGM - Initialized to 0 in latest VB code

        self.kc_bas_prev = 0.

        # TP - Added

        self.max_lines_in_crop_curve_table = 34

        # CGM - In VB code, crops 44-46 were run first to set these values kn kcb_daily()
        #   Initialize here instead
        #   Using a dictionary instead of an array to make the indexing more obvious

        self.kc_bas_wscc = dict()
        self.kc_bas_wscc[1] = 0.1
        self.kc_bas_wscc[2] = 0.1
        self.kc_bas_wscc[3] = 0.1

        # TP - Minimum net depth of application for germination irrigation, etc.

        self.irr_min = 10.

    def crop_load(self, et_cell):
        """Assign characteristics for crop from crop Arrays
        Parameters
        ---------
        data : dict
            configuration data from INI file

        et_cell :

        crop :


        Returns
        -------
        None

        Notes
        -----
        Called by crop_cycle.crop_cycle() just before time loop

        """

        self.height_min = 0.1
        self.height_max = 2.0
        self.zr_min = 0.1
        self.zr_max = 1.7

        self.depl_ze = de_initial  # (10 mm) at start of new crop at beginning of time
        self.depl_root = de_initial  # (20 mm) at start of new crop at beginning of time
        self.zr = self.zr_min  # initialize rooting depth at beginning of time
        self.height = self.height_min
        self.stress_event = False

        # Find maximum kc_bas in array for this crop (used later in height calc)
        # kc_bas_mid is the maximum kc_bas found in the kc_bas table read into program
        # Following code was repaired to properly parse crop curve arrays on 7/31/2012.  dlk

        self.kc_bas_mid = 0.

        # Available water in soil
        self.aw = et_cell.props['stn_whc'] / 12 * 1000.  # in/ft to mm/m

        # Estimate readily evaporable water and total evaporable water from WHC
        # REW is from regression of REW vs. AW from FAO-56 soils table
        # R.Allen, August 2006, R2=0.92, n = 9

        self.rew = 0.8 + 54.4 * self.aw / 1000  # REW is in mm and AW is in mm/m

        # Estimate TEW from AW and Ze = 0.1 m
        # use FAO-56 based regression, since WHC from statso database does not have texture indication
        # R.Allen, August 2006, R2=0.88, n = 9

        self.tew = -3.7 + 166 * self.aw / 1000  # TEW is in mm and AW is in mm/m
        if self.rew > (0.8 * self.tew):
            self.rew = 0.8 * self.tew  # limit REW based on TEW
        self.tew2 = self.tew  # TEW2Array(ctCount)
        self.tew3 = self.tew  # TEW3Array(ctCount) '(no severely cracking clays in Idaho)
        self.kr2 = 0  # Kr2Array(ctCount)'(no severely cracking clays in Idaho)

        self.setup_crop()

    def setup_crop(self):
        """Initialize some variables for beginning of crop seasons

        Attributes
        ----------
        crop :


        Returns
        -------
        None

        Notes
        -----
        Called in crop_cycle if not in season and crop setup flag is true
        Called in kcb_daily for startup/greenup type 1, 2, and 3 when startup conditions are met

        """

        # zr_dormant was never assigned a value - what's its purpose - dlk 10/26/2011 ???????????????????

        zr_dormant = 0.0

        # setup_crop is called from crop_cycle if is_season is false and crop_setup_flag is true
        # thus only setup 1st time for crop (not each year)
        # also called from kcb_daily each time GU/Plant date is reached, thus at growing season start

        self.height = self.height_min
        self.tew = self.tew2  # find total evaporable water
        if self.tew < self.tew3:
            self.tew = self.tew3
        self.fw_irr = self.fw_std  # fw changed to fw_irr 8/10/06
        self.irr_auto = 0
        self.irr_sim = 0

        # Reinitialize zr, but actCount for additions of DP into reserve (zrmax - zr) for rainfed

        # Convert current moisture content below Zr at end of season to AW for new crop
        # (into starting moisture content of layer 3).  This is required if zr_min != zr_dormant
        # Calc total water currently in layer 3

        # AW3 is mm/m and daw3 is mm in layer 3 (in case Zr<zr_max)

        daw3 = self.aw3 * (self.zr_max - zr_dormant)

        # Layer 3 is soil depth between current rootzone (or dormant rootdepth) and max root for crop
        # AW3 is set to 0 first time throught for crop.

        # Potential water in root zone below zr_dormant

        taw3 = self.aw * (self.zr_max - zr_dormant)

        # Make sure that AW3 has been collecting DP from zr_dormant layer during winter

        # if daw3 < 0.0: daw3 = 0.
        daw3 = max(0, daw3)
        # if taw3 < 0.0: taw3 = 0.
        taw3 = max(0, taw3)
        if self.zr_min > zr_dormant:
            # adjust depletion for extra starting root zone at plant or GU
            # assume fully mixed layer 3

            self.depl_root = (
                self.depl_root + (taw3 - daw3) *
                (self.zr_min - zr_dormant) / (self.zr_max - zr_dormant))
        elif self.zr_max > self.zr_min:
            # Was, until 5/9/07:
            # Assume moisture right above zr_dormant is same as below
            # depl_root = depl_root - (taw3 - daw3) * (zr_dormant - zr_min) / (zr_max - zr_min)
            # Following added 5/9/07
            # Enlarge depth of water

            daw3 = (
                daw3 + (zr_dormant - self.zr_min) / zr_dormant *
                (self.aw * zr_dormant - self.depl_root))

            # Adjust depl_root in proportion to zr_min / zdormant and increase daw3 and AW3

            self.depl_root *= self.zr_min / zr_dormant

            # denom is layer 3 depth at start of season

            self.aw3 = daw3 / (self.zr_max - self.zr_min)
            # if self.aw3 < 0.0: self.aw3 = 0.
            self.aw3 = max(0.0, self.aw3)
            if self.aw3 > self.aw: self.aw3 = self.aw
            self.aw3 = min(self.aw, self.aw3)
        if self.depl_root < 0.: self.depl_root = 0.

        # Initialize rooting depth at beginning of time  <----DO??? Need recalc on Reserve?
        self.zr = self.zr_min
        self.crop_setup_flag = False

    def setup_dormant(self, et_cell, crop):
        """Start of dormant season
            a) Set up for soil water reservoir during non-growing season
                to collect soil moisture for next growing season
            b) Set for type of surface during non-growing season

        Parameters
        ---------
        et_cell :

        crop :

        Returns
        -------
        None

        Notes
        -----
        Called at termination of crop from crop_cycle()
        If in_season is false and dormant_flag is true,
        dormant_flag set at GU each year.
        It is called each year as soon as season is 0.

        """

        # winter_surface_cover_class = 1 bare, 2 mulch, 3 sod

        wscc = crop.winter_surface_cover_class

        # Kc_bas for wintertime land use
        #  44: Bare soil
        #  45: Mulched soil, including wheat stubble
        #  46: Dormant turf/sod (winter time)
        #  note: set Kcmax for winter time (Nov-Mar) and fc outside of this sub.

        if wscc == 1:
            self.kc_bas = 0.1  # was 0.2
            self.fc = 0
        elif wscc == 2:
            self.kc_bas = 0.1  # was 0.2
            self.fc = 0.4
        elif wscc == 3:
            self.kc_bas = 0.2  # was 0.3
            self.fc = 0.7  # was 0.6

        # Setup curve number for antecedent II condition for winter covers
        # Crop params dictionary uses crop number as key
        # Don't subtract 1 to convert to an index
        if et_cell.stn_hydrogroup == 1:
            self.cn2 = et_cell.crop_params[wscc + 43].cn_coarse_soil
        elif et_cell.stn_hydrogroup == 2:
            self.cn2 = et_cell.crop_params[wscc + 43].cn_medium_soil
        elif et_cell.stn_hydrogroup == 3:
            self.cn2 = et_cell.crop_params[wscc + 43].cn_fine_soil

        # Assume that 'rooting depth' for dormant surfaces is 0.1 or 0.15 m
        # This is depth that will be applied with a stress function to reduce kc_bas

        zr_dormant = 0.1  # was 0.15

        # Convert current moisture content of Zr layer
        #   (which should be at zr_max at end of season)
        #   into starting moisture content of layer 3
        # This is done at end of season

        # Calc total water currently in layer 3 (the dynamic layer below zr)
        # AW is mm/m and daw3 is mm in layer 3 (in case zr < zr_max)

        daw3 = self.aw3 * (self.zr_max - self.zr)

        # Add TAW - depl_root that is in root zone below zr_dormant.
        # Assume fully mixed root zone including zr_dormant part

        # Potential water in root zone

        taw_root = self.aw * (self.zr)

        # Actual water in root zone based on depl_root at end of season

        daw_root = max(taw_root - self.depl_root, 0)

        # Depth of evaporation layer (This only works when ze < zr_dormant)

        ze = 0.1

        # Reduce daw_root by water in evap layer and rest of zr_dormant and then proportion
        if zr_dormant < self.zr:
            # determine water in zr_dormant layer
            # combine water in ze layer (1-fc fraction) to that in balance of zr_dormant depth
            # need to mix ze and zr_dormant zones.  Assume current Zr zone of crop just ended is fully mixed.
            # totwatin_ze is water in fc fraction of Ze.

            aw_root = daw_root / self.zr
            if zr_dormant > ze:
                totwatinzr_dormant = (
                    (self.totwatin_ze + aw_root * (zr_dormant - ze)) * (1 - self.fc) +
                    aw_root * zr_dormant * self.fc)
            else:
                # Was, until 5/9/07
                # totwatinzr_dormant = (
                #     .totwatin_ze * (ze - zr_dormant) / ze) * (1 - fc) +
                #     _root * zr_dormant * fc)
                totwatinzr_dormant = (
                    (self.totwatin_ze * (1 - (ze - zr_dormant) / ze)) * (1 - self.fc) +
                    aw_root * zr_dormant * self.fc)  # corrected

            # This requires that zr_dormant > ze.

            if daw_root > totwatinzr_dormant:
                # Proportionate water between zr_dormant and zr

                daw_below = (daw_root - totwatinzr_dormant)

                # Actual water between zr_dormant and zr
                # daw_below = daw_root * (zr - zr_dormant) / zr
            else:
                daw_below = 0

            # Actual water in mm/m below zr_dormant

            self.aw3 = (daw_below + daw3) / (self.zr_max - zr_dormant)
        else:
            # This should never happen, since zr_max for all crops > 0.15 m

            self.aw3 = self.aw3

        # initialize depl_root for dormant season
        # Depletion below evaporation layer:

        # depl_root_below_Ze = (depl_root - de)  # / (zr - ze) #'mm/m
        # If depl_root_below_ze < 0 Then depl_root_below_ze = 0
        # assume fully mixed profile below Ze
        # depl_root = depl_root_below_ze * (zr_dormant - ze) / (zr - ze) + de

        self.depl_root = self.aw * zr_dormant - totwatinzr_dormant

        # set Zr for dormant season

        self.zr = zr_dormant

        # This value for zr will hold constant all dormant season.  dp from zr will be
        # used to recharge zr_max - zr zone.
        # Make sure that grow_root is not called during dormant season.

        self.fw_irr = self.fw_std  # fw changed to fw_irr 8/10/06
        self.irr_auto = 0
        self.irr_sim = 0
        self.dormant_setup_flag = False

        self.cutting = 0

    def setup_dataframe(self, et_cell, fmt):
        """Initialize output dataframe

        Attributes
        ----------
        et_cell :


        Returns
        -------

        Notes
        -----

        """

        self.crop_df = et_cell.input[['year', 'month', 'day', 'doy']].copy()
        self.crop_df['et_act'] = np.nan
        self.crop_df['et_pot'] = np.nan
        self.crop_df['et_bas'] = np.nan
        self.crop_df['kc_act'] = np.nan
        self.crop_df['kc_bas'] = np.nan
        self.crop_df['irrigation'] = np.nan
        self.crop_df['runoff'] = np.nan
        self.crop_df['dperc'] = np.nan
        self.crop_df['niwr'] = np.nan
        self.crop_df['season'] = 0
        self.crop_df['cutting'] = 0
        add_cols = [c for c in fmt if c not in self.crop_df.columns]
        for c in add_cols:
            self.crop_df[c] = np.nan

    def set_kc_max(self, et_cell):

        kc_array = et_cell.input[['etf_inv_irr', 'etf_irr']].quantile(0.9) * self.etf_coeff
        self.kc_max = kc_array.max().item()
