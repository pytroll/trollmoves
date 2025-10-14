"""Test utility functions."""

import copy
import datetime
import os
import unittest
from tempfile import gettempdir

from posttroll.message import Message

from trollmoves.utils import gen_dict_extract, translate_dict, translate_dict_item, translate_dict_value

test_msg = 'pytroll://segment/SDR/1B/nrk/prod/polar/direct_readout dataset safuser@lxserv1131.smhi.se 2018-10-25T01:15:54.752065 v1.01 application/json {"sensor": "viirs", "format": "SDR", "variant": "DR", "start_time": "2018-10-25T01:01:46", "orbit_number": 36230, "dataset": [{"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/GMTCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011335298494_cspp_dev.h5", "uid": "GMTCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011335298494_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM01_npp_d20181025_t0101464_e0103106_b36230_c20181025011354163052_cspp_dev.h5", "uid": "SVM01_npp_d20181025_t0101464_e0103106_b36230_c20181025011354163052_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM02_npp_d20181025_t0101464_e0103106_b36230_c20181025011354178693_cspp_dev.h5", "uid": "SVM02_npp_d20181025_t0101464_e0103106_b36230_c20181025011354178693_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354194042_cspp_dev.h5", "uid": "SVM03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354194042_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354209273_cspp_dev.h5", "uid": "SVM04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354209273_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354224550_cspp_dev.h5", "uid": "SVM05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354224550_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM06_npp_d20181025_t0101464_e0103106_b36230_c20181025011354240108_cspp_dev.h5", "uid": "SVM06_npp_d20181025_t0101464_e0103106_b36230_c20181025011354240108_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM07_npp_d20181025_t0101464_e0103106_b36230_c20181025011354256470_cspp_dev.h5", "uid": "SVM07_npp_d20181025_t0101464_e0103106_b36230_c20181025011354256470_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM08_npp_d20181025_t0101464_e0103106_b36230_c20181025011354291614_cspp_dev.h5", "uid": "SVM08_npp_d20181025_t0101464_e0103106_b36230_c20181025011354291614_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM09_npp_d20181025_t0101464_e0103106_b36230_c20181025011354320585_cspp_dev.h5", "uid": "SVM09_npp_d20181025_t0101464_e0103106_b36230_c20181025011354320585_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM10_npp_d20181025_t0101464_e0103106_b36230_c20181025011354337251_cspp_dev.h5", "uid": "SVM10_npp_d20181025_t0101464_e0103106_b36230_c20181025011354337251_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM11_npp_d20181025_t0101464_e0103106_b36230_c20181025011354366238_cspp_dev.h5", "uid": "SVM11_npp_d20181025_t0101464_e0103106_b36230_c20181025011354366238_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM12_npp_d20181025_t0101464_e0103106_b36230_c20181025011354382899_cspp_dev.h5", "uid": "SVM12_npp_d20181025_t0101464_e0103106_b36230_c20181025011354382899_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM13_npp_d20181025_t0101464_e0103106_b36230_c20181025011354407042_cspp_dev.h5", "uid": "SVM13_npp_d20181025_t0101464_e0103106_b36230_c20181025011354407042_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM14_npp_d20181025_t0101464_e0103106_b36230_c20181025011354448503_cspp_dev.h5", "uid": "SVM14_npp_d20181025_t0101464_e0103106_b36230_c20181025011354448503_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM15_npp_d20181025_t0101464_e0103106_b36230_c20181025011354478025_cspp_dev.h5", "uid": "SVM15_npp_d20181025_t0101464_e0103106_b36230_c20181025011354478025_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM16_npp_d20181025_t0101464_e0103106_b36230_c20181025011354506942_cspp_dev.h5", "uid": "SVM16_npp_d20181025_t0101464_e0103106_b36230_c20181025011354506942_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/GITCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333965280_cspp_dev.h5", "uid": "GITCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333965280_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI01_npp_d20181025_t0101464_e0103106_b36230_c20181025011353975082_cspp_dev.h5", "uid": "SVI01_npp_d20181025_t0101464_e0103106_b36230_c20181025011353975082_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI02_npp_d20181025_t0101464_e0103106_b36230_c20181025011353990747_cspp_dev.h5", "uid": "SVI02_npp_d20181025_t0101464_e0103106_b36230_c20181025011353990747_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354006115_cspp_dev.h5", "uid": "SVI03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354006115_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354022377_cspp_dev.h5", "uid": "SVI04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354022377_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354093439_cspp_dev.h5", "uid": "SVI05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354093439_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/GDNBO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333695023_cspp_dev.h5", "uid": "GDNBO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333695023_cspp_dev.h5"}, {"uri": "ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVDNB_npp_d20181025_t0101464_e0103106_b36230_c20181025011353771298_cspp_dev.h5", "uid": "SVDNB_npp_d20181025_t0101464_e0103106_b36230_c20181025011353771298_cspp_dev.h5"}], "platform_name": "Suomi-NPP", "orig_orbit_number": 36230, "end_time": "2018-10-25T01:03:10", "type": "HDF5", "data_processing_level": "1B"}'  # noqa
DEST_DIR = gettempdir()


def dummy_callback_translate_dict_values(k, v):
    dirname, filename = os.path.split(v)
    return os.path.join(DEST_DIR, filename)


def dummy_callback_translate_dict_item(var, k):
    dirname, filename = os.path.split(var[k])
    basename, ext = os.path.splitext(filename)
    if dirname:
        dest = os.path.join(DEST_DIR, basename + ".png")
    else:
        dest = basename + ".png"
    var[k] = dest
    return var


def dummy_callback_translate_dict_item_to_dataset(var):
    if not var["uid"].endswith(".tar"):
        return var
    dirname, filename = os.path.split(var.pop("uri"))
    basename, ext = os.path.splitext(filename)
    new_names = [basename + str(i) + ".png" for i in range(1, 3)]
    var.pop("uid")
    var["dataset"] = [dict(uid=nn, uri=os.path.join(DEST_DIR, nn)) for nn in new_names]
    return var


class TestWalker(unittest.TestCase):
    """Test walker."""

    def test_gen_dict_extract_message(self):
        """Test extracting info from message data dictionary."""
        orig = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                '{"sensor": "viirs", "format": "SDR", "variant": "DR", "uid": "bla.png", "uri": "/home/user/bla.png"}')
        expected_uid = ["bla.png"]

        res = list(gen_dict_extract(Message(rawstr=orig).data, "uid"))

        self.assertListEqual(expected_uid, res)

    def test_gen_dict_extract_dictionary(self):
        """Test extracting info from dictionary."""
        test_dict = {'sensor': 'viirs', 'format': 'SDR', 'variant': 'DR', 'start_time': datetime.datetime(2018, 10, 25, 1, 1, 46), 'orbit_number': 36230, 'dataset': [{'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/GMTCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011335298494_cspp_dev.h5', 'uid': 'GMTCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011335298494_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM01_npp_d20181025_t0101464_e0103106_b36230_c20181025011354163052_cspp_dev.h5', 'uid': 'SVM01_npp_d20181025_t0101464_e0103106_b36230_c20181025011354163052_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM02_npp_d20181025_t0101464_e0103106_b36230_c20181025011354178693_cspp_dev.h5', 'uid': 'SVM02_npp_d20181025_t0101464_e0103106_b36230_c20181025011354178693_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354194042_cspp_dev.h5', 'uid': 'SVM03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354194042_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354209273_cspp_dev.h5', 'uid': 'SVM04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354209273_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354224550_cspp_dev.h5', 'uid': 'SVM05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354224550_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM06_npp_d20181025_t0101464_e0103106_b36230_c20181025011354240108_cspp_dev.h5', 'uid': 'SVM06_npp_d20181025_t0101464_e0103106_b36230_c20181025011354240108_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM07_npp_d20181025_t0101464_e0103106_b36230_c20181025011354256470_cspp_dev.h5', 'uid': 'SVM07_npp_d20181025_t0101464_e0103106_b36230_c20181025011354256470_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM08_npp_d20181025_t0101464_e0103106_b36230_c20181025011354291614_cspp_dev.h5', 'uid': 'SVM08_npp_d20181025_t0101464_e0103106_b36230_c20181025011354291614_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM09_npp_d20181025_t0101464_e0103106_b36230_c20181025011354320585_cspp_dev.h5', 'uid': 'SVM09_npp_d20181025_t0101464_e0103106_b36230_c20181025011354320585_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM10_npp_d20181025_t0101464_e0103106_b36230_c20181025011354337251_cspp_dev.h5', 'uid': 'SVM10_npp_d20181025_t0101464_e0103106_b36230_c20181025011354337251_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM11_npp_d20181025_t0101464_e0103106_b36230_c20181025011354366238_cspp_dev.h5', 'uid': 'SVM11_npp_d20181025_t0101464_e0103106_b36230_c20181025011354366238_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM12_npp_d20181025_t0101464_e0103106_b36230_c20181025011354382899_cspp_dev.h5', 'uid': 'SVM12_npp_d20181025_t0101464_e0103106_b36230_c20181025011354382899_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM13_npp_d20181025_t0101464_e0103106_b36230_c20181025011354407042_cspp_dev.h5', 'uid': 'SVM13_npp_d20181025_t0101464_e0103106_b36230_c20181025011354407042_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM14_npp_d20181025_t0101464_e0103106_b36230_c20181025011354448503_cspp_dev.h5', 'uid': 'SVM14_npp_d20181025_t0101464_e0103106_b36230_c20181025011354448503_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM15_npp_d20181025_t0101464_e0103106_b36230_c20181025011354478025_cspp_dev.h5', 'uid': 'SVM15_npp_d20181025_t0101464_e0103106_b36230_c20181025011354478025_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVM16_npp_d20181025_t0101464_e0103106_b36230_c20181025011354506942_cspp_dev.h5', 'uid': 'SVM16_npp_d20181025_t0101464_e0103106_b36230_c20181025011354506942_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/GITCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333965280_cspp_dev.h5', 'uid': 'GITCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333965280_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI01_npp_d20181025_t0101464_e0103106_b36230_c20181025011353975082_cspp_dev.h5', 'uid': 'SVI01_npp_d20181025_t0101464_e0103106_b36230_c20181025011353975082_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI02_npp_d20181025_t0101464_e0103106_b36230_c20181025011353990747_cspp_dev.h5', 'uid': 'SVI02_npp_d20181025_t0101464_e0103106_b36230_c20181025011353990747_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354006115_cspp_dev.h5', 'uid': 'SVI03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354006115_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354022377_cspp_dev.h5', 'uid': 'SVI04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354022377_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVI05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354093439_cspp_dev.h5', 'uid': 'SVI05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354093439_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/GDNBO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333695023_cspp_dev.h5', 'uid': 'GDNBO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333695023_cspp_dev.h5'}, {'uri': 'ssh://lxserv1131.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20181025_0048_36230/SVDNB_npp_d20181025_t0101464_e0103106_b36230_c20181025011353771298_cspp_dev.h5', 'uid': 'SVDNB_npp_d20181025_t0101464_e0103106_b36230_c20181025011353771298_cspp_dev.h5'}], 'platform_name': 'Suomi-NPP', 'orig_orbit_number': 36230, 'end_time': datetime.datetime(2018, 10, 25, 1, 3, 10), 'type': 'HDF5', 'data_processing_level': '1B', 'request_address': '10.120.1.40:9099'}  # noqa
        expected = [
            "GMTCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011335298494_cspp_dev.h5",
            "SVM01_npp_d20181025_t0101464_e0103106_b36230_c20181025011354163052_cspp_dev.h5",
            "SVM02_npp_d20181025_t0101464_e0103106_b36230_c20181025011354178693_cspp_dev.h5",
            "SVM03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354194042_cspp_dev.h5",
            "SVM04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354209273_cspp_dev.h5",
            "SVM05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354224550_cspp_dev.h5",
            "SVM06_npp_d20181025_t0101464_e0103106_b36230_c20181025011354240108_cspp_dev.h5",
            "SVM07_npp_d20181025_t0101464_e0103106_b36230_c20181025011354256470_cspp_dev.h5",
            "SVM08_npp_d20181025_t0101464_e0103106_b36230_c20181025011354291614_cspp_dev.h5",
            "SVM09_npp_d20181025_t0101464_e0103106_b36230_c20181025011354320585_cspp_dev.h5",
            "SVM10_npp_d20181025_t0101464_e0103106_b36230_c20181025011354337251_cspp_dev.h5",
            "SVM11_npp_d20181025_t0101464_e0103106_b36230_c20181025011354366238_cspp_dev.h5",
            "SVM12_npp_d20181025_t0101464_e0103106_b36230_c20181025011354382899_cspp_dev.h5",
            "SVM13_npp_d20181025_t0101464_e0103106_b36230_c20181025011354407042_cspp_dev.h5",
            "SVM14_npp_d20181025_t0101464_e0103106_b36230_c20181025011354448503_cspp_dev.h5",
            "SVM15_npp_d20181025_t0101464_e0103106_b36230_c20181025011354478025_cspp_dev.h5",
            "SVM16_npp_d20181025_t0101464_e0103106_b36230_c20181025011354506942_cspp_dev.h5",
            "GITCO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333965280_cspp_dev.h5",
            "SVI01_npp_d20181025_t0101464_e0103106_b36230_c20181025011353975082_cspp_dev.h5",
            "SVI02_npp_d20181025_t0101464_e0103106_b36230_c20181025011353990747_cspp_dev.h5",
            "SVI03_npp_d20181025_t0101464_e0103106_b36230_c20181025011354006115_cspp_dev.h5",
            "SVI04_npp_d20181025_t0101464_e0103106_b36230_c20181025011354022377_cspp_dev.h5",
            "SVI05_npp_d20181025_t0101464_e0103106_b36230_c20181025011354093439_cspp_dev.h5",
            "GDNBO_npp_d20181025_t0101464_e0103106_b36230_c20181025011333695023_cspp_dev.h5",
            "SVDNB_npp_d20181025_t0101464_e0103106_b36230_c20181025011353771298_cspp_dev.h5"]

        self.assertListEqual(expected, list(gen_dict_extract(test_dict, "uid")))

    def test_translate_dict_values_file_message(self):
        """Test translating message dictionary values."""
        orig = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                '{"sensor": "viirs", "format": "SDR", "variant": "DR", "uid": "bla.png", "uri": "/home/user/bla.png"}')
        expected = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                    '{"sensor": "viirs", "format": "SDR", "variant": "DR", "uid": "bla.png", "uri": "/tmp/bla.png"}')

        expected_dict = Message(rawstr=expected).data
        res = translate_dict_value(Message(rawstr=orig).data, "uri", dummy_callback_translate_dict_values)
        self.assertDictEqual(expected_dict, res)

    def test_translate_dict_values_dataset_message(self):
        """Test translating message dictionary values."""
        orig = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                '{"sensor": "viirs", "format": "SDR", "variant": "DR", '
                ' "dataset": [{"uid": "bla.png", "uri": "/home/user/bla.png"},'
                '             {"uid": "bla2.png", "uri": "/home/user/bla2.png"}]}')

        expected = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                    '{"sensor": "viirs", "format": "SDR", "variant": "DR", '
                    ' "dataset": [{"uid": "bla.png", "uri": "/tmp/bla.png"},'
                    '             {"uid": "bla2.png", "uri": "/tmp/bla2.png"}]}')

        expected_dict = Message(rawstr=expected).data
        res = translate_dict_value(Message(rawstr=orig).data, "uri", dummy_callback_translate_dict_values)
        self.assertDictEqual(expected_dict, res)

    def test_translate_dict_values_collection_message(self):
        """Test translating message dictionary values."""
        orig = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                '{"sensor": "viirs", "format": "SDR", "variant": "DR", '
                ' "collection": [{"dataset": [{"uid": "bla.png", "uri": "/home/user/bla.png"},'
                '                             {"uid": "bla2.png", "uri": "/home/user/bla2.png"}]}]}')

        expected = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                    '{"sensor": "viirs", "format": "SDR", "variant": "DR", '
                    ' "collection": [{"dataset": [{"uid": "bla.png", "uri": "/tmp/bla.png"},'
                    '                             {"uid": "bla2.png", "uri": "/tmp/bla2.png"}]}]}')

        expected_dict = Message(rawstr=expected).data
        res = translate_dict_value(Message(rawstr=orig).data, "uri", dummy_callback_translate_dict_values)
        self.assertDictEqual(expected_dict, res)

    def test_translate_dict_value(self):
        """Test message data dictionary value translation."""
        msg = Message(rawstr=test_msg)
        orig_data = copy.deepcopy(msg.data)

        res = translate_dict_value(msg.data, "uri", dummy_callback_translate_dict_values)
        for uri in gen_dict_extract(res, "uri"):
            self.assertTrue(uri.startswith(DEST_DIR))

        self.assertDictEqual(orig_data, msg.data)

    def test_translate_dict_item(self):
        """Test translating dict items."""
        orig = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                '{"sensor": "viirs", "format": "SDR", "variant": "DR", "uid": "bla.tar", "uri": "/home/user/bla.tar"}')
        expected = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                    '{"sensor": "viirs", "format": "SDR", "variant": "DR", "uid": "bla.png", "uri": "/tmp/bla.png"}')

        expected_dict = Message(rawstr=expected).data
        res = translate_dict_item(Message(rawstr=orig).data, "uri", dummy_callback_translate_dict_item)
        res = translate_dict_item(res, "uid", dummy_callback_translate_dict_item)
        self.assertDictEqual(expected_dict, res)

    def test_translate_dict_item_to_dataset(self):
        """Test translating dict items to dataset."""
        orig = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                '{"sensor": "viirs", "format": "SDR", "variant": "DR", "uid": "bla.tar", "uri": "/home/user/bla.tar"}')
        expected = ('pytroll://tm1 file s@lx.serv.com 2018-10-25T01:15:54.752065 v1.01 application/json '
                    '{"sensor": "viirs", "format": "SDR", "variant": "DR", "dataset": [{"uid": "bla1.png", "uri": "/tmp/bla1.png"}, {"uid": "bla2.png", "uri": "/tmp/bla2.png"}]}')  # noqa

        expected_dict = Message(rawstr=expected).data
        res = translate_dict(Message(rawstr=orig).data, ("uri", "uid"), dummy_callback_translate_dict_item_to_dataset)
        self.assertDictEqual(expected_dict, res)


if __name__ == "__main__":
    unittest.main()
