"""
Test our groupby support based on the pandas groupby tests.
"""
#
# This file is licensed under the Pandas 3 clause BSD license.
#

from sparklingpandas.test.sp_test_case import \
    SparklingPandasTestCase
from pandas import bdate_range
from pandas.core.index import Index, MultiIndex
from pandas.core.api import DataFrame
from pandas.core.series import Series
from pandas.util.testing import assert_frame_equal
from pandas import compat
import pandas.util.testing as tm
import unittest2
import numpy as np

try:
    # rands was moved to util.testing in pandas 0.15
    from pandas.core.common import rands  # pylint: disable=no-name-in-module
except ImportError:
    from pandas.util.testing import rands


class PandasGroupby(SparklingPandasTestCase):
    def setUp(self):
        """
        Setup the dataframes used for the groupby tests derived from pandas
        """
        self.date_rng = bdate_range('1/1/2005', periods=250)
        self.string_idx = Index([rands(8).upper() for x in range(250)])

        self.group_id = Series([x[0] for x in self.string_idx],
                               index=self.string_idx)
        self.group_dict = dict((key, value) for key, value in
                               compat.iteritems(self.group_id))

        self.col_idx = Index(['A', 'B', 'C', 'D', 'E'])

        rand_matrix = np.random.randn(250, 5)
        self.string_matrix = DataFrame(rand_matrix, columns=self.col_idx,
                                       index=self.string_idx)

        self.time_matrix = DataFrame(rand_matrix, columns=self.col_idx,
                                     index=self.date_rng)
        self.time_series = tm.makeTimeSeries()

        self.seriesd = tm.getSeriesData()
        self.tsd = tm.getTimeSeriesData()
        self.frame = DataFrame(self.seriesd)
        self.tsframe = DataFrame(self.tsd)

        self.pd_df_foobar = DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
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
        ddf = self.psc.from_pd_data_frame(self.pd_df_foobar)
        assert_frame_equal(ddf.collect(), self.pd_df_foobar)
        grouped = self.psc.from_pd_data_frame(self.pd_df_foobar).groupby('A')
        first = grouped.first().collect()
        expected = self.pd_df_foobar.ix[[1, 0], ['B', 'C', 'D']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(first, expected)

        nth = grouped.nth(0).collect()
        assert_frame_equal(nth, expected)

        last = grouped.last().collect()
        expected = self.pd_df_foobar.ix[[5, 7], ['B', 'C', 'D']]
        expected.index = Index(['bar', 'foo'], name='A')
        assert_frame_equal(last, expected)

        nth = grouped.nth(-1).collect()
        assert_frame_equal(nth, expected)

        nth = grouped.nth(1).collect()
        expected = self.pd_df_foobar.ix[[2, 3], ['B', 'C', 'D']].copy()
        expected.index = Index(['foo', 'bar'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(nth, expected)

    @unittest2.expectedFailure
    def test_new_in0140(self):
        """
        Test new functionality in 0.14.0. This currently doesn't work.
        """
        # v0.14.0 whatsnew
        input_df = DataFrame([[1, np.nan], [1, 4], [5, 6]], columns=['A', 'B'])
        sp_df = self.psc.from_pd_data_frame(input_df)
        grouped_sp_df = sp_df.groupby('A')
        result = grouped_sp_df.first().collect()
        expected = input_df.iloc[[1, 2]].set_index('A')
        assert_frame_equal(result, expected)

        expected = input_df.iloc[[1, 2]].set_index('A')
        result = grouped_sp_df.nth(0, dropna='any').collect()
        assert_frame_equal(result, expected)

    @unittest2.expectedFailure
    def test_first_last_nth_dtypes(self):
        """
        We do groupby fine on mixed types, but our copy from local dataframe
        ends up re-running the guess type function, so the dtypes don't match.
        Issue #25
        """
        sp_df = self.df_mixed_floats.copy()
        sp_df['E'] = True
        sp_df['F'] = 1

        # tests for first / last / nth
        grouped = sp_df.groupby('A')
        first = grouped.first().collect()
        expected = sp_df.ix[[1, 0], ['B', 'C', 'D', 'E', 'F']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(first, expected)

        last = grouped.last().collect()
        expected = sp_df.ix[[5, 7], ['B', 'C', 'D', 'E', 'F']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(last, expected)

        nth = grouped.nth(1).collect()
        expected = sp_df.ix[[3, 2], ['B', 'C', 'D', 'E', 'F']]
        expected.index = Index(['bar', 'foo'], name='A')
        expected = expected.sort_index()
        assert_frame_equal(nth, expected)

    def test_var_on_multiplegroups(self):
        pd_df = DataFrame({'data1': np.random.randn(5),
                           'data2': np.random.randn(5),
                           'data3': np.random.randn(5),
                           'key1': ['a', 'a', 'b', 'b', 'a'],
                           'key2': ['one', 'two', 'one', 'two', 'one']})
        sp_df = self.psc.from_pd_data_frame(pd_df)
        actual_grouped = sp_df.groupby(['key1', 'key2'])
        expected_grouped = pd_df.groupby(['key1', 'key2'])
        assert_frame_equal(actual_grouped.var().collect(),
                           expected_grouped.var())

    def test_agg_api(self):
        # Note: needs a very recent version of pandas to pass
        # TODO(holden): Pass this test if local fails
        # GH 6337
        # http://stackoverflow.com/questions/21706030/pandas-groupby-agg-function-column-dtype-error
        # different api for agg when passed custom function with mixed frame

        pd_df = DataFrame({'data1': np.random.randn(5),
                           'data2': np.random.randn(5),
                           'key1': ['a', 'a', 'b', 'b', 'a'],
                           'key2': ['one', 'two', 'one', 'two', 'one']})
        ddf = self.psc.from_pd_data_frame(pd_df)
        dgrouped = ddf.groupby('key1')
        grouped = pd_df.groupby('key1')

        def peak_to_peak(arr):
            return arr.max() - arr.min()

        expected = grouped.agg([peak_to_peak])
        expected.columns = ['data1', 'data2']
        result = dgrouped.agg(peak_to_peak).collect()
        assert_frame_equal(result, expected)

    def test_agg_regression1(self):
        grouped = self.tsframe.groupby([lambda x: x.year, lambda x: x.month])
        dgrouped = self.psc.from_pd_data_frame(
            self.tsframe).groupby([lambda x: x.year, lambda x: x.month])
        result = dgrouped.agg(np.mean).collect()
        expected = grouped.agg(np.mean)
        assert_frame_equal(result, expected)
