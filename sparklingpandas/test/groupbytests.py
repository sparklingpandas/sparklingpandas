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

from tempfile import NamedTemporaryFile
from sparklingpandas.test.sparklingpandastestcase import \
    SparklingPandasTestCase
import sys
import pandas as pd
import unittest2
import numpy as np


class Groupby(SparklingPandasTestCase):

    """Test groupby functionality. Uses the standard dataframe from
    L{pandasparktestcase}.
    """

    def test_basic_groupby(self):
        """Test groupby with out sorting."""
        groupedFrame = self.basicframe.groupby('magic', sort=False)
        distributedGroupedFrame = self.basicpframe.groupby('magic', sort=False)
        distributedGroupedFrame._cache()
        self._compare_groupby_results(groupedFrame, distributedGroupedFrame)

    def test_basic_groupby_numeric(self):
        """Test groupby with out sorting."""
        groupedFrame = self.numericframe.groupby('a', sort=False)
        distributedGroupedFrame = self.numericpframe.groupby('a', sort=False)
        distributedGroupedFrame._cache()
        self._compare_groupby_results(groupedFrame, distributedGroupedFrame)

    def test_sorted_groupby(self):
        """Test groupby with sorting."""
        groupedFrame = self.basicframe.groupby('magic', sort=True)
        distributedGroupedFrame = self.basicpframe.groupby('magic', sort=True)
        distributedGroupedFrame._cache()
        self._compare_groupby_results(groupedFrame,
                                      distributedGroupedFrame, order=True)

    def test_twocol_groupby(self):
        """Test to make sure that groupby works in two columns mode."""
        groupedFrame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        distributedGroupedFrame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        distributedGroupedFrame._cache()
        self._compare_groupby_results(groupedFrame,
                                      distributedGroupedFrame, order=True)

    def test_numeric_groupby(self):
        """Test to make sure that groupby works with numeric data."""
        numericGroupedFrame = self.numericframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericpframe.groupby(
            'a', sort=True)
        self._compare_groupby_results(
            numericGroupedFrame,
            distributedNumericGroupedFrame,
            order=True)

    def test_groups(self):
        """Test to make sure we get the same groups back from distributed
        and local.
        """
        groupedFrame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        distributedGroupedFrame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        self.assertEquals(groupedFrame.groups,
                          distributedGroupedFrame.groups)

    def test_ngroups(self):
        """Test to make sure we get the same number of groups back from
        distributed and local.
        """
        groupedFrame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        distributedGroupedFrame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        self.assertEquals(
            groupedFrame.ngroups, distributedGroupedFrame.ngroups)

    def test_indices(self):
        """Test to make sure we get the same group indices back from distributed
        and local
        """
        groupedFrame = self.basicframe.groupby(['magic', 'thing'], sort=True)
        distributedGroupedFrame = self.basicpframe.groupby(
            ['magic', 'thing'], sort=True)
        self.assertEquals(groupedFrame.indices,
                          distributedGroupedFrame.indices)

    def test_sum(self):
        """Test that sum works on a numeric data frame."""
        numericGroupedFrame = self.numericframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericpframe.groupby(
            'a', sort=True)
        self._compareDataFrames(numericGroupedFrame.sum(),
                                distributedNumericGroupedFrame.sum().collect())

    def test_median(self):
        """Test that median works on a numeric data frame."""
        numericGroupedFrame = self.numericframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericpframe.groupby(
            'a', sort=True)
        self._compareDataFrames(
            numericGroupedFrame.median(),
            distributedNumericGroupedFrame.median().collect())

    def test_mean(self):
        """Test that mean works on a numeric data frame."""
        numericGroupedFrame = self.numericframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericpframe.groupby(
            'a', sort=True)
        self._compareDataFrames(
            numericGroupedFrame.mean(),
            distributedNumericGroupedFrame.mean().collect())

    def test_var(self):
        """Test that var works on a numeric data frame."""
        numericGroupedFrame = self.numericframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericpframe.groupby(
            'a', sort=True)
        self._compareDataFrames(numericGroupedFrame.var(),
                                distributedNumericGroupedFrame.var().collect())

    def test_sum_three_col(self):
        """Test that sum works on three column numeric data frame."""
        numericGroupedFrame = self.numericthreeframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericthreepframe.groupby(
            'a', sort=True)
        self._compareDataFrames(numericGroupedFrame.sum(),
                                distributedNumericGroupedFrame.sum().collect())

    def test_min(self):
        """Test that min works on a numeric data frame."""
        numericGroupedFrame = self.numericframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericpframe.groupby(
            'a', sort=True)
        self._compareDataFrames(numericGroupedFrame.min(),
                                distributedNumericGroupedFrame.min().collect())

    def test_apply(self):
        """Test that apply works on a numeric data frame."""
        numericGroupedFrame = self.numericframe.groupby('a', sort=True)
        distributedNumericGroupedFrame = self.numericpframe.groupby(
            'a', sort=True)
        self._compareDataFrames(
            numericGroupedFrame.apply(
                lambda x: x), distributedNumericGroupedFrame.apply(
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
            for (k1, v1), (k2, v2) in zip(grl1, grl2):
                self.assertEqual(k1, k2)
                self._compareDataFrames(v1, v2)
        else:
            for (k1, v1), (k2, v2) in zip(gr1, gr2):
                self.assertEqual(k1, k2)
                self._compareDataFrames(v1, v2)

if __name__ == "__main__":
    unittest2.main()
