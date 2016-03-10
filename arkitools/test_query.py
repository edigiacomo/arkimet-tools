import unittest

from .query import TrangeGrib1Expander, LevelGrib1Expander


class TestTrangeGrib1Expander(unittest.TestCase):
    def test_analysis(self):
        self.assertEqual(
            TrangeGrib1Expander().add(1, None, None, None).query(),
            "timerange: GRIB1,1,0h"
        )
        self.assertEqual(
            str(TrangeGrib1Expander().add(1, None, None, None)),
            "timerange: GRIB1,1,0h"
        )

    def test_forecast_tr_0(self):
        self.assertEqual(
            str(TrangeGrib1Expander().add(0, 3, 5, 1)),
            "timerange: GRIB1,0,3h or GRIB1,0,4h or GRIB1,0,5h"
        )

    def test_forecast_tr_2(self):
        self.assertEqual(
            str(TrangeGrib1Expander().add(2, 3, 5, 1)),
            "timerange: GRIB1,2,0h,3h or GRIB1,2,0h,4h or GRIB1,2,0h,5h"
        )
        self.assertEqual(
            str(TrangeGrib1Expander().add(2, 3, 5, 1, False)),
            "timerange: GRIB1,2,3h,4h or GRIB1,2,4h,5h or GRIB1,2,5h,6h"
        )

    def test_multiple(self):
        self.assertEqual(
            str(TrangeGrib1Expander().add(1, None, None, None).add(0, 3, 5, 1)),
            "timerange: GRIB1,1,0h or GRIB1,0,3h or GRIB1,0,4h or GRIB1,0,5h"
        )

class TestLevelGrib1Expander(unittest.TestCase):
    def test_abs(self):
        self.assertEqual(
            str(LevelGrib1Expander().add(1)),
            "level: GRIB1,1"
        )
