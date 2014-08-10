"""
Test our groupby support based on the pandas groupby tests.
"""
#
# This file is licensed under the Pandas 3 clause BSD license.
#

from tempfile import NamedTemporaryFile
from sparklingpandas.test.sparklingpandastestcase import \
    SparklingPandasTestCase
import sys
import pandas as pd
from pandas import date_range, bdate_range, Timestamp
from pandas.core.index import Index, MultiIndex, Int64Index
from pandas.core.common import rands
from pandas.core.api import Categorical, DataFrame
from pandas.core.series import Series
from pandas.util.testing import (assert_panel_equal, assert_frame_equal,
                                 assert_series_equal, assert_almost_equal,
                                 assert_index_equal, assertRaisesRegexp)
from pandas.compat import(
    range, long, lrange, StringIO, lmap, lzip, map,
    zip, builtins, OrderedDict
)
from pandas import compat
import pandas.util.testing as tm
import unittest2
import numpy as np


class PandasGroupby(SparklingPandasTestCase):

    def setUp(self):
        """
        Setup the dataframes used for the groupby tests derived from pandas
        """
        self.dateRange = bdate_range('1/1/2005', periods=250)
        self.stringIndex = Index([rands(8).upper() for x in range(250)])

        self.groupId = Series([x[0] for x in self.stringIndex],
                              index=self.stringIndex)
        self.groupDict = dict((k, v)
                              for k, v in compat.iteritems(self.groupId))

        self.columnIndex = Index(['A', 'B', 'C', 'D', 'E'])

        randMat = np.random.randn(250, 5)
        self.stringMatrix = DataFrame(randMat, columns=self.columnIndex,
                                      index=self.stringIndex)

        self.timeMatrix = DataFrame(randMat, columns=self.columnIndex,
                                    index=self.dateRange)
        self.ts = tm.makeTimeSeries()

        self.seriesd = tm.getSeriesData()
        self.tsd = tm.getTimeSeriesData()
        self.frame = DataFrame(self.seriesd)
        self.tsframe = DataFrame(self.tsd)

        self.df = DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
                                   'foo', 'bar', 'foo', 'foo'],
                             'B': ['one', 'one', 'two', 'three',
                                   'two', 'two', 'one', 'three'],
                             'C': np.random.randn(8),
                             'D': np.random.randn(8)})

        self.df_mixed_floats = DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
                                                'foo', 'bar', 'foo', 'foo'],
                                          'B': ['one', 'one', 'two', 'three',
                                                'two', 'two', 'one', 'three'],
                                          'C': np.random.randn(8),
                                          'D': np.array(np.random.randn(8),
                                                        dtype='float32')})

        index = MultiIndex(levels=[['foo', 'bar', 'baz', 'qux'],
                                   ['one', 'two', 'three']],
                           labels=[[0, 0, 0, 1, 1, 2, 2, 3, 3, 3],
                                   [0, 1, 2, 0, 1, 1, 2, 0, 1, 2]],
                           names=['first', 'second'])
        self.mframe = DataFrame(np.random.randn(10, 3), index=index,
                                columns=['A', 'B', 'C'])

        self.three_group = DataFrame({'A': ['foo', 'foo', 'foo', 'foo',
                                            'bar', 'bar', 'bar', 'bar',
                                            'foo', 'foo', 'foo'],
                                      'B': ['one', 'one', 'one', 'two',
                                            'one', 'one', 'one', 'two',
                                            'two', 'two', 'one'],
                                      'C': ['dull', 'dull', 'shiny', 'dull',
                                            'dull', 'shiny', 'shiny', 'dull',
                                            'shiny', 'shiny', 'shiny'],
                                      'D': np.random.randn(11),
                                      'E': np.random.randn(11),
                                      'F': np.random.randn(11)})
        super(self.__class__, self).setUp()

    def test_first_last_nth(self):
        # tests for first / last / nth
        ddf = self.psc.from_data_frame(self.df)
        assert_frame_equal(ddf.collect(), self.df)
        grouped = self.psc.from_data_frame(self.df).groupby('A')
        first = grouped.first().collect()
        expected = self.df.ix[[1, 0], ['B', 'C', 'D']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(first, expected)

        nth = grouped.nth(0).collect()
        assert_frame_equal(nth, expected)

        last = grouped.last().collect()
        expected = self.df.ix[[5, 7], ['B', 'C', 'D']]
        expected.index = Index(['bar', 'foo'], name='A')
        assert_frame_equal(last, expected)

        nth = grouped.nth(-1).collect()
        assert_frame_equal(nth, expected)

        nth = grouped.nth(1).collect()
        expected = self.df.ix[[2, 3], ['B', 'C', 'D']].copy()
        expected.index = Index(['foo', 'bar'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(nth, expected)

    @unittest2.expectedFailure
    def test_getitem(self):
        # it works!
        grouped['B'].first()
        grouped['B'].last()
        grouped['B'].nth(0)

        self.df.loc[self.df['A'] == 'foo', 'B'] = np.nan
        self.assertTrue(com.isnull(grouped['B'].first()['foo']))
        self.assertTrue(com.isnull(grouped['B'].last()['foo']))
        # not sure what this is testing
        self.assertTrue(com.isnull(grouped['B'].nth(0)[0]))

    @unittest2.expectedFailure
    def test_new_in0140(self):
        """
        Test new functionality in 0.14.0. This currently doesn't work.
        """
        # v0.14.0 whatsnew
        df = DataFrame([[1, np.nan], [1, 4], [5, 6]], columns=['A', 'B'])
        ddf = self.psc.from_data_frame(df)
        g = ddf.groupby('A')
        result = g.first().collect()
        expected = df.iloc[[1, 2]].set_index('A')
        assert_frame_equal(result, expected)

        expected = df.iloc[[1, 2]].set_index('A')
        result = g.nth(0, dropna='any').collect()
        assert_frame_equal(result, expected)

    @unittest2.expectedFailure
    def test_first_last_nth_dtypes(self):
        """
        We do groupby fine on mixed types, but our copy from local dataframe
        ends up re-running the guess type function, so the dtypes don't match.
        Issue #25
        """
        df = self.df_mixed_floats.copy()
        df['E'] = True
        df['F'] = 1

        # tests for first / last / nth
        grouped = ddf.groupby('A')
        first = grouped.first().collect()
        expected = df.ix[[1, 0], ['B', 'C', 'D', 'E', 'F']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(first, expected)

        last = grouped.last().collect()
        expected = df.ix[[5, 7], ['B', 'C', 'D', 'E', 'F']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(last, expected)

        nth = grouped.nth(1).collect()
        expected = df.ix[[3, 2], ['B', 'C', 'D', 'E', 'F']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(nth, expected)

    def test_var_on_multiplegroups(self):
        df = DataFrame({'data1': np.random.randn(5),
                        'data2': np.random.randn(5),
                        'data3': np.random.randn(5),
                        'key1': ['a', 'a', 'b', 'b', 'a'],
                        'key2': ['one', 'two', 'one', 'two', 'one']})
        ddf = self.psc.from_data_frame(df)
        dgrouped = ddf.groupby(['key1', 'key2'])
        grouped = df.groupby(['key1', 'key2'])
        assert_frame_equal(dgrouped.var().collect(), grouped.var())

    def test_agg_api(self):
        # Note: needs a very recent version of pandas to pass
        # TODO(holden): Pass this test if local fails
        # GH 6337
        # http://stackoverflow.com/questions/21706030/pandas-groupby-agg-function-column-dtype-error
        # different api for agg when passed custom function with mixed frame

        df = DataFrame({'data1': np.random.randn(5),
                        'data2': np.random.randn(5),
                        'key1': ['a', 'a', 'b', 'b', 'a'],
                        'key2': ['one', 'two', 'one', 'two', 'one']})
        ddf = self.psc.from_data_frame(df)
        dgrouped = ddf.groupby('key1')
        grouped = df.groupby('key1')

        def peak_to_peak(arr):
            return arr.max() - arr.min()

        expected = grouped.agg([peak_to_peak])
        expected.columns = ['data1', 'data2']
        result = dgrouped.agg(peak_to_peak).collect()
        assert_frame_equal(result, expected)

    def test_agg_regression1(self):
        grouped = self.tsframe.groupby([lambda x: x.year, lambda x: x.month])
        dgrouped = self.psc.from_data_frame(
            self.tsframe).groupby(
            [lambda x: x.year, lambda x: x.month])
        result = dgrouped.agg(np.mean).collect()
        expected = grouped.agg(np.mean)
        assert_frame_equal(result, expected)
