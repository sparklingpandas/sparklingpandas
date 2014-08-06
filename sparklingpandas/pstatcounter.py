"""
This module provides statistics for L{PRDD}s.
Look at the stats() method on PRDD for more info.
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

from sparklingpandas.utils import add_pyspark_path
import pandas

add_pyspark_path()

from pyspark.statcounter import StatCounter


class PStatCounter(object):

    """
    A wrapper around StatCounter which collects stats for multiple columns
    """

    def __init__(self, dataframes, columns):
        """
        Creates a stats counter for the provided data frames
        computing the stats for all of the columns in columns.
        Parameters
        ----------
        dataframes: list of dataframes, containing the values to compute stats
                on.
        columns: list of strs, list of columns to compute the stats on.
        """
        assert (not isinstance(columns, basestring)), "columns should be a " \
                                                      "list of strs,  " \
                                                      "not a str!"
        assert isinstance(columns, list), "columns should be a list!"

        self._columns = columns
        self._counters = dict((column, StatCounter()) for column in columns)

        for df in dataframes:
            self.merge(df)

    def merge(self, frame):
        """
        Add another DataFrame to the PStatCounter.
        """
        for column, values in frame.iteritems():
            # Temporary hack, fix later
            counter = self._counters.get(column)
            for value in values:
                if counter is not None:
                    counter.merge(value)

    def merge_pstats(self, other):
        """
        Merge all of the stats counters of the other PStatCounter with our
        counters.
        """
        if not isinstance(other, PStatCounter):
            raise Exception("Can only merge PStatcounters!")

        for column, counter in self._counters.items():
            other_counter = other._counters.get(column)
            self._counters[column] = counter.mergeStats(other_counter)

        return self

    def __str__(self):
        str = ""
        for column, counter in self._counters.items():
            str += "(field: %s,  counters: %s)" % (column, counter)
        return str

    def __repr__(self):
        return self.__str__()


class ColumnStatCounters(object):

    """
    A wrapper around StatCounter which collects stats for multiple columns
    """

    def __init__(self, dataframes=[], columns=[]):
        """
        Creates a stats counter for the provided data frames
        computing the stats for all of the columns in columns.
        Parameters
        ----------
        dataframes: list of dataframes, containing the values to compute stats
        on columns: list of strs, list of columns to compute the stats on
        """
        self._column_stats = dict((column_name, StatCounter()) for
                                  column_name in columns)

        for df in dataframes:
            self.merge(df)

    def merge(self, frame):
        """
        Add another DataFrame to the accumulated stats for each column.
        Parameters
        ----------
        frame: pandas DataFrame we will update our stats counter with.
        """
        for column_name, counter in self._column_stats.items():
            data_arr = frame[[column_name]].values
            count, min_max_tup, mean, unbiased_var, skew, kurt = \
                scistats.describe(data_arr)
            stats_counter = StatCounter()
            stats_counter.n = count
            stats_counter.mu = mean
            # TODO(juliet): look up paper they base their streams tat alg on,
            # write docs for statcounter class in spark
            # line below will likely need to be modified to match the alg
            stats_counter.m2 = np.sum((data_arr - mean) ** 2)
            stats_counter.minValue, stats_counter.maxValue = min_max_tup
            self._column_stats[column_name] = self._column_stats[
                column_name].mergeStats(stats_counter)
        return self

    def merge_stats(self, other_col_counters):
        """
        Merge statistics from a different column stats counter in to this one.
        Parameters
        ----------
        other_column_counters: Other col_stat_counter to marge in to this one.
        """
        for column_name, counter in self._column_stats.items():
            self._column_stats[column_name] = self._column_stats[column_name] \
                .mergeStats(other_col_counters._column_stats[column_name])
        return self

    def __str__(self):
        str = ""
        for column, counter in self._column_stats.items():
            str += "(field: %s,  counters: %s)" % (column, counter)
        return str

    def __repr__(self):
        return self.__str__()
