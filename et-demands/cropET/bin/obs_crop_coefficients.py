"""crop_coefficients.py
Defines CropCoeffs class
Defines functions read_crop_coefs_txt and read_crop_coefs_xls_xlrd to read
    crop coefficient data
Called by crop_et_data.py

"""

import numpy as np

"""
curve_descs : dict
    Crop curve type dictionary
    NCGDD (1)
    %PL-EC (2)
    %PL-EC+daysafter (3)
    %PL-Term (4)

"""

curve_descs = {'1': '1=NCGDD', '2': '2=%PL-EC', '3': '3=%PL-EC+daysafter', '4': '4=%PL-Term', '5': '5=NDVI'}


class ObsCropCoeff:
    """Crop coefficient container

    Attributes
    ----------
        curve_no :
            Crop curve number (1-60)
        curve_type_no :
            Crop curve type (1-4)
        curve_type :
            Crop curve type number
            (NCGDD, %PL-EC, %PL-EC+daysafter, %PL-Term)
        name : string
            Crop name
        data : ndarray
            Crop coefficient curve values

    Notes
    -----
    See comments in code

    """

    def __init__(self, curve_type_no):
        """ """
        self.name = None
        self.gdd_type_name = ''

        self.curve_type_no = curve_type_no.replace('.0', '')
        self.curve_types = curve_descs[self.curve_type_no]
        self.name = curve_descs[self.curve_type_no]

    def __str__(self):
        """ """
        return '<%s, type %s>' % (self.name, self.curve_types)


if __name__ == '__main__':
    pass
