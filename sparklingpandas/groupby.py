"""Provide wrapper around the grouped result from L{PRDD}"""
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

from sparklingpandas.utils import add_pyspark_path
from sparklingpandas.prdd import PRDD
add_pyspark_path()
import pandas
import numpy as np


class GroupBy:

    """An RDD with key value pairs, where each value is a Panda's dataframe and
    the key is the result of the group. Supports many of the same operations
    as a Panda's GroupBy."""

    def __init__(self, prdd, *args, **kwargs):
        """Construct a groupby object providing the functions on top of the
        provided RDD. We keep the base RDD so if someone calls aggregate we
        do things more intelligently.
        """
        def extract_keys(groupedFrame):
            for key, group in groupedFrame:
                yield (key, group)

        def group_and_extract(frame):
            return extract_keys(frame.groupby(*args, **kwargs))

        self._sort = kwargs.get("sort", True)
        self._by = kwargs.get("by", args[0])
        self._prdd = prdd

    def prep_old_school(self):
        self._baseRDD = self._rdd()
        self._distributedRDD = rdd.flatMap(group_and_extract)
        self._mergedRDD = self._sortIfNeeded(
            self._group(self._distributedRDD))
        self._myargs = args
        self._mykwargs = kwargs

    def _sortIfNeeded(self, rdd):
        """Sort by key if we need to."""
        if self._sort:
            return rdd.sortByKey()
        else:
            return rdd

    def _group(self, rdd):
        """Group together the values with the same key."""
        return rdd.reduceByKey(lambda x, y: x.append(y))

    def __len__(self):
        """Number of groups."""
        if (self._fast_code)
        return self.prdd.to_spark_sql.groupby(self._by).count()

    def get_group(self, name):
        """Returns a concrete DataFrame for provided group name."""
        self._mergedRDD.lookup(name)

    def __iter__(self):
        """Returns an iterator of (name, dataframe) to the local machine.
        """
        return self._mergedRDD.collect().__iter__()

    def collect(self):
        """Return a list of the elements. This is a SparklingPanda extension
        because Spark gives us back a list we convert to an iterator in
        __iter__ so it allows us to skip the round trip through iterators.
        """
        return self._mergedRDD.collect()

    @property
    def groups(self):
        """Returns dict {group name -> group labels}."""
        def extract_group_labels(frame):
            return (frame[0], frame[1].index.values)
        return self._mergedRDD.map(extract_group_labels).collectAsMap()

    @property
    def ngroups(self):
        """Number of groups."""
        return self._mergedRDD.count()

    @property
    def indices(self):
        """Returns dict {group name -> group indices}."""
        def extract_group_indices(frame):
            return (frame[0], frame[1].index)
        return self._mergedRDD.map(extract_group_indices).collectAsMap()

    def median(self):
        """Compute median of groups, excluding missing values.

        For multiple groupings, the result index will be a MultiIndex.
        """
        return PRDD.fromRDD(
            self._regroup_mergedRDD().values().map(
                lambda x: x.median()))

    def mean(self):
        """Compute mean of groups, excluding missing values.

        For multiple groupings, the result index will be a MultiIndex.
        """
        # TODO(holden): use stats counter
        return PRDD.fromRDD(
            self._regroup_mergedRDD().values().map(
                lambda x: x.mean()))

    def var(self, ddof=1):
        """Compute standard deviation of groups, excluding missing values.

        For multiple groupings, the result index will be a MultiIndex.
        """
        # TODO(holden): use stats counter
        return PRDD.fromRDD(
            self._regroup_mergedRDD().values().map(
                lambda x: x.var(
                    ddof=ddof)))

    def sum(self):
        """Compute the sum for each group."""
        myargs = self._myargs
        mykwargs = self._mykwargs

        def create_combiner(x):
            return x.groupby(*myargs, **mykwargs).sum()

        def merge_value(x, y):
            return pandas.concat([x, create_combiner(y)])

        def merge_combiner(x, y):
            return x + y

        rddOfSum = self._sortIfNeeded(self._distributedRDD.combineByKey(
            create_combiner,
            merge_value,
            merge_combiner)).values()
        return PRDD.fromRDD(rddOfSum)

    def min(self):
        """Compute the min for each group."""
        myargs = self._myargs
        mykwargs = self._mykwargs

        def create_combiner(x):
            return x.groupby(*myargs, **mykwargs).min()

        def merge_value(x, y):
            return x.append(create_combiner(y)).min()

        def merge_combiner(x, y):
            return x.append(y).min(level=0)

        rddOfMin = self._sortIfNeeded(self._distributedRDD.combineByKey(
            create_combiner,
            merge_value,
            merge_combiner)).values()
        return PRDD.fromRDD(rddOfMin)

    def max(self):
        """Compute the max for each group."""
        myargs = self._myargs
        mykwargs = self._mykwargs

        def create_combiner(x):
            return x.groupby(*myargs, **mykwargs).max()

        def merge_value(x, y):
            return x.append(create_combiner(y)).max()

        def merge_combiner(x, y):
            return x.append(y).max(level=0)

        rddOfMax = self._sortIfNeeded(self._distributedRDD.combineByKey(
            create_combiner,
            merge_value,
            merge_combiner)).values()
        return PRDD.fromRDD(rddOfMax)

    def first(self):
        """
        Pull out the first from each group. Note: this is different than
        Spark's first.
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def create_combiner(x):
            return x.groupby(*myargs, **mykwargs).first()

        def merge_value(x, y):
            return create_combiner(x)

        def merge_combiner(x, y):
            return x

        rddOfFirst = self._sortIfNeeded(self._distributedRDD.combineByKey(
            create_combiner,
            merge_value,
            merge_combiner)).values()
        return PRDD.fromRDD(rddOfFirst)

    def last(self):
        """Pull out the last from each group."""
        myargs = self._myargs
        mykwargs = self._mykwargs

        def create_combiner(x):
            return x.groupby(*myargs, **mykwargs).last()

        def merge_value(x, y):
            return create_combiner(y)

        def merge_combiner(x, y):
            return y

        rddOfLast = self._sortIfNeeded(self._distributedRDD.combineByKey(
            create_combiner,
            merge_value,
            merge_combiner)).values()
        return PRDD.fromRDD(rddOfLast)

    def _regroup_mergedRDD(self):
        """A common pattern is we want to call groupby again on the dataframes
        so we can use the groupby functions.
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def regroup(df):
            return df.groupby(*myargs, **mykwargs)

        return self._mergedRDD.mapValues(regroup)

    def nth(self, n, *args, **kwargs):
        """Take the nth element of each grouby."""
        # TODO: Stop collecting the entire frame for each key.
        myargs = self._myargs
        mykwargs = self._mykwargs
        nthRDD = self._regroup_mergedRDD().mapValues(
            lambda r: r.nth(
                n, *args, **kwargs)).values()
        return PRDD.fromRDD(nthRDD)

    def aggregate(self, f):
        """Apply the aggregation function.
        Note: This implementation does note take advantage of partial
        aggregation.
        """
        return PRDD.fromRDD(
            self._regroup_mergedRDD().values().map(
                lambda g: g.aggregate(f)))

    def agg(self, f):
        return self.aggregate(f)

    def apply(self, func, *args, **kwargs):
        """Apply the provided function and combine the results together in the
        same way as apply from groupby in pandas.

        This returns a PRDD.
        """
        def key_by_index(data):
            """Key each row by its index.
            """
            # TODO: Is there a better way to do this?
            for key, row in data.iterrows():
                yield (key, pandas.DataFrame.from_dict(dict([(key, row)]),
                                                       orient='index'))

        myargs = self._myargs
        mykwargs = self._mykwargs
        regroupedRDD = self._distributedRDD.mapValues(
            lambda data: data.groupby(*myargs, **mykwargs))
        appliedRDD = regroupedRDD.map(
            lambda key_data: key_data[1].apply(func, *args, **kwargs))
        reKeyedRDD = appliedRDD.flatMap(key_by_index)
        prdd = self._sortIfNeeded(reKeyedRDD).values()
        return PRDD.fromRDD(prdd)
