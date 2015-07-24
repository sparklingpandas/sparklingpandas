"""Test our groupby support"""
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from sparklingpandas.test.sp_test_case import \
    SparklingPandasTestCase

import unittest2
from pandas.util.testing import assert_frame_equal


class Groupby(SparklingPandasTestCase):

    """Test groupby functionality. Uses the standard dataframe from
    L{pandasparktestcase}.
    """

    def test_basic_groupby(self):
        """Test groupby with out sorting."""
        grouped_frame = self.basicframe.groupby('magic', sort=False)
        dist_grouped_frame = self.basicpframe.groupby('magic', sort=False)
        self._compare_groupby_results(grouped_frame, dist_grouped_frame)

    @unittest2.skip("Skipping this test.")
    def test_basic_groupby_first(self):
        """Test groupby with out sorting."""
        grouped_frame = self.basicframe.groupby('magic', sort=False)
        dist_grouped_frame = self.basicpframe.groupby('magic', sort=False)
        first_grouped_frame = grouped_frame.first()
        first_dist_grouped_frame = dist_grouped_frame.first()
        assert_frame_equal(first_grouped_frame,
                           first_dist_grouped_frame.collect())

    @unittest2.skip("Spark SQL 1.4.0 & 1.4.1 has some issues with nulls")
    def test_basic_groupby_na(self):
        """Test groupby with out sorting on an na frame"""
        grouped_frame = self.mixednaframe.groupby('a', sort=False)
        dist_grouped_frame = self.mixednapframe.groupby('a', sort=False)
        self._compare_groupby_results(grouped_frame, dist_grouped_frame)

    def test_basic_groupby_numeric(self):
        """Test groupby with out sorting."""
        grouped_frame = self.numericframe.groupby('a', sort=False)
        dist_grouped_frame = self.numericpframe.groupby('a', sort=False)
        self._compare_groupby_results(grouped_frame, dist_grouped_frame)

    def test_sorted_groupby(self):
        """Test groupby with sorting."""
        grouped_frame = self.basicframe.groupby('magic', sort=True)
        dist_grouped_frame = self.basicpframe.groupby('magic', sort=True)
        self._compare_groupby_results(grouped_frame,
                                      dist_grouped_frame, order=True)

    def test_twocol_groupby(self):
        """Test to make sure that groupby works in two columns mode."""
        grouped_frame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        dist_grouped_frame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        self._compare_groupby_results(grouped_frame,
                                      dist_grouped_frame, order=True)

    def test_numeric_groupby(self):
        """Test to make sure that groupby works with numeric data."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_groupby_results(
            numeric_grouped_frame,
            dist_num_grouped_frame,
            order=True)

    def test_groups(self):
        """Test to make sure we get the same groups back from distributed
        and local.
        """
        grouped_frame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        dist_grouped_frame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        self.assertEquals(grouped_frame.groups,
                          dist_grouped_frame.groups)

    def test_ngroups(self):
        """Test to make sure we get the same number of groups back from
        distributed and local.
        """
        grouped_frame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        dist_grouped_frame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        self.assertEquals(
            grouped_frame.ngroups, dist_grouped_frame.ngroups)

    def test_indices(self):
        """Test to make sure we get the same group indices back from distributed
        and local
        """
        grouped_frame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        dist_grouped_frame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        self.assertEquals(grouped_frame.indices,
                          dist_grouped_frame.indices)

    def test_sum(self):
        """Test that sum works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_dfs(numeric_grouped_frame.sum(),
                          dist_num_grouped_frame.sum().collect())

    @unittest2.skip("Type trouble with the median aggregator.")
    def test_median(self):
        """Test that median works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_dfs(
            numeric_grouped_frame.median(),
            dist_num_grouped_frame.median().collect())

    def test_mean(self):
        """Test that mean works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_dfs(
            numeric_grouped_frame.mean(),
            dist_num_grouped_frame.mean().collect())

    def test_var(self):
        """Test that var works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_dfs(numeric_grouped_frame.var(),
                          dist_num_grouped_frame.var().collect())

    def test_kurtosis(self):
        """Test that kurtosis works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        from pandas import Series
        expected = numeric_grouped_frame.aggregate(Series.kurtosis)
        result = dist_num_grouped_frame.aggregate(Series.kurtosis)
        self._compare_dfs(expected, result.collect())

    def test_sum_three_col(self):
        """Test that sum works on three column numeric data frame."""
        numeric_grouped_frame = self.numericthreeframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericthreepframe.groupby(
            'a', sort=True)
        self._compare_dfs(numeric_grouped_frame.sum(),
                          dist_num_grouped_frame.sum().collect())

    def test_min(self):
        """Test that min works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_dfs(numeric_grouped_frame.min(),
                          dist_num_grouped_frame.min().collect())

    def test_max(self):
        """Test that max works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_dfs(numeric_grouped_frame.max(),
                          dist_num_grouped_frame.max().collect())

    def test_apply(self):
        """Test that apply works on a numeric data frame."""
        numeric_grouped_frame = self.numericframe.groupby('a', sort=True)
        dist_num_grouped_frame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_dfs(numeric_grouped_frame.apply(lambda x: x),
                          dist_num_grouped_frame.apply(
                              lambda x: x).collect())

    def _compare_groupby_results(self, gr1, gr2, order=False):
        """ Compare the results of two groupbys. By default sorts the results
        before comparing. Specify order=True to check that the results are
        in the same order.
        """
        self.assertEqual(
            gr1.__len__(),
            gr2.__len__(),
            "The group by results have the same length")
        if not order:
            grl1 = list(gr1.__iter__())
            grl2 = list(gr2.__iter__())
            grl1.sort()
            grl2.sort()
            for (key1, value1), (key2, value2) in zip(grl1, grl2):
                self.assertEqual(key1, key2)
                self._compare_dfs(value1, value2)
        else:
            for (key1, value1), (key2, value2) in zip(gr1, gr2):
                self.assertEqual(key1, key2)
                self._compare_dfs(value1, value2)

if __name__ == "__main__":
    unittest2.main()
