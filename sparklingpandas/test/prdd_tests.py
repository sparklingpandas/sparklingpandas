"""
Test methods in prdd
"""
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from sparklingpandas.test.sparklingpandastestcase import \
    SparklingPandasTestCase

import pandas as pd
import numpy.testing as np_tests


class PContextTests(SparklingPandasTestCase):

    def test_apply_map(self):
        input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        prdd = self.psc.DataFrame(input, columns=['magic', 'thing'])
        addpandasfunc = (lambda x: "panda" + x)
        result = prdd.applymap(addpandasfunc).collect()

        expected_thing_result = ["pandahappy", "pandasad", "pandahappiest"]
        expected_magic_result = ["pandatea", "pandawater", "pandacoffee"]
        actual_thing_result = result["thing"].values.tolist()
        actual_magic_result = result["magic"].values.tolist()

        assert isinstance(result, type(pd.DataFrame()))
        assert expected_magic_result == actual_magic_result
        assert expected_thing_result == actual_thing_result

    def test_get_item(self):
        input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        prdd = self.psc.DataFrame(input, columns=['magic', 'thing'])
        actual_col = prdd['thing'].collect()
        actual_thing_result = actual_col.values.tolist()
        expected_thing_result = ["happy", "sad", "happiest"]
        assert expected_thing_result == actual_thing_result

    def test_collect(self):
        input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        prdd = self.psc.DataFrame(input, columns=['magic', 'thing'])
        collected_result = prdd.collect()

        expected_thing_result = ["happy", "sad", "happiest"]
        expected_magic_result = ["tea", "water", "coffee"]
        actual_thing_result = collected_result["thing"].values.tolist()
        actual_magic_result = collected_result["magic"].values.tolist()

        assert isinstance(collected_result, type(pd.DataFrame()))
        assert expected_magic_result == actual_magic_result
        assert expected_thing_result == actual_thing_result

    def test_stats(self):
        input = [("magic", 10), ("ninja", 20), ("coffee", 30)]
        prdd = self.psc.DataFrame(input, columns=['a', 'b'])
        stats = prdd.stats(columns=['b'])
        b_col_stat_counter = stats._counters['b']
        np_tests.assert_almost_equal(b_col_stat_counter.count(), 3)
        np_tests.assert_almost_equal(b_col_stat_counter.mean(), 20.0)
        np_tests.assert_almost_equal(b_col_stat_counter.stdev(), 8.16496580928)
        np_tests.assert_almost_equal(b_col_stat_counter.max(), 30)
        np_tests.assert_almost_equal(b_col_stat_counter.min(), 10)
