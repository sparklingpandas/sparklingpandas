"""
Provide wrapper around the grouped result from L{PRDD}
"""
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


class Groupby:

    """
    An RDD with key value pairs, where each value is a dataframe and the key is
    the result of the group.
    """
    # TODO(HOLDEN): finish implement the rest of the non "protected" functions
    # in groupby

    def __init__(self, rdd, *args, **kwargs):
        """
        Construct a groupby object providing the functions on top of the
        provided RDD. We keep the base RDD so if someone calls aggregate we
        do things more inteligently
        """
        def extractKeys(groupedFrame):
            for key, group in groupedFrame:
                yield (key, group)

        def group_and_extract(frame):
            return extractKeys(frame.groupby(*args, **kwargs))
        prereducedRDD = rdd.flatMap(group_and_extract)
        groupedRDD = self._groupRDD(prereducedRDD)
        self._sort = kwargs.get("sort", True)
        if self._sort:
            groupedRDD = groupedRDD.sortByKey()
        self._baserdd = rdd
        self._prereducedrdd = prereducedRDD
        self._groupedrdd = groupedRDD
        self._myargs = args
        self._mykwargs = kwargs

    def _sortIfNeeded(self, rdd):
        """Sort by key if we need to"""
        if self._sort:
            return rdd.sortByKey()
        else:
            return rdd

    def _groupRDD(self, rdd):
        """Group together the values with the same key"""
        return rdd.reduceByKey(lambda x, y: x.append(y))

    def _cache(self):
        """
        Cache the grouped RDD. This is useful if you have multiple computations
        to run on the result. This is a SparklingPandas extension
        """
        self._groupedrdd.cache()

    def __len__(self):
        """Number of groups"""
        return self._groupedrdd.count()

    def get_group(self, name):
        """
        Returns a concrete DataFrame for provided group name
        """
        self._groupedrdd.lookup(name)

    def __iter__(self):
        """
        Groupby iterator returns a sequence of (name, object) for each group.
        Note: this brings the entire result back to your driver program
        """
        return self._groupedrdd.collect().__iter__()

    def collect(self):
        """
        Return a list of the elements. This is a SparklingPanda extension
        because Spark gives us back a list we convert to an iterator in
        __iter__ so it allows us to skip the round trip through iterators.
        """
        return self._groupedrdd.collect()

    @property
    def groups(self):
        """ dict {group name -> group labels} """
        return self._groupedrdd.map(
            lambda
            key_frame:
            (key_frame[0],
             key_frame[1].index.values)).collectAsMap()

    @property
    def ngroups(self):
        """ Number of groups """
        return self._groupedrdd.count()

    @property
    def indices(self):
        """ dict {group name -> group indices} """
        return self._groupedrdd.map(
            lambda
            key_frame1:
            (key_frame1[0],
             key_frame1[1].index)).collectAsMap()

    def median(self):
        """
        Compute median of groups, excluding missing values

        For multiple groupings, the result index will be a MultiIndex
        """
        # TODO(holden): use stats counter
        return PRDD.fromRDD(
            self._regroup_groupedrdd().values().map(
                lambda x: x.median()))

    def mean(self):
        """
        Compute mean of groups, excluding missing values

        For multiple groupings, the result index will be a MultiIndex
        """
        # TODO(holden): use stats counter
        return PRDD.fromRDD(
            self._regroup_groupedrdd().values().map(
                lambda x: x.mean()))

    def var(self, ddof=1):
        """
        Compute standard deviation of groups, excluding missing values

        For multiple groupings, the result index will be a MultiIndex
        """
        # TODO(holden): use stats counter
        return PRDD.fromRDD(
            self._regroup_groupedrdd().values().map(
                lambda x: x.var(
                    ddof=ddof)))

    def sum(self):
        """
        Compute the sum for each group
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def createCombiner(x):
            return x.groupby(*myargs, **mykwargs).sum()

        def mergeValue(x, y):
            return pandas.concat([x, createCombiner(y)])

        def mergeCombiner(x, y):
            return x + y
        rddOfSum = self._sortIfNeeded(self._prereducedrdd.combineByKey(
            createCombiner,
            mergeValue,
            mergeCombiner)).values()
        return PRDD.fromRDD(rddOfSum)

    def min(self):
        """
        Compute the min for each group
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def createCombiner(x):
            return x.groupby(*myargs, **mykwargs).min()

        def mergeValue(x, y):
            return x.append(createCombiner(y)).min()

        def mergeCombiner(x, y):
            return x.append(y).min(level=0)
        rddOfMin = self._sortIfNeeded(self._prereducedrdd.combineByKey(
            createCombiner,
            mergeValue,
            mergeCombiner)).values()
        return PRDD.fromRDD(rddOfMin)

    def max(self):
        """
        Compute the max for each group
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def createCombiner(x):
            return x.groupby(*myargs, **mykwargs).max()

        def mergeValue(x, y):
            return x.append(createCombiner(y)).max()

        def mergeCombiner(x, y):
            return x.append(y).max(level=0)
        rddOfMax = self._sortIfNeeded(self._prereducedrdd.combineByKey(
            createCombiner,
            mergeValue,
            mergeCombiner)).values()
        return PRDD.fromRDD(rddOfMax)

    def first(self):
        """
        Pull out the first
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def createCombiner(x):
            return x.groupby(*myargs, **mykwargs).first()

        def mergeValue(x, y):
            return createCombiner(x)

        def mergeCombiner(x, y):
            return x

        rddOfFirst = self._sortIfNeeded(self._prereducedrdd.combineByKey(
            createCombiner,
            mergeValue,
            mergeCombiner)).values()
        return PRDD.fromRDD(rddOfFirst)

    def last(self):
        """
        Pull out the last
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def createCombiner(x):
            return x.groupby(*myargs, **mykwargs).last()

        def mergeValue(x, y):
            return createCombiner(y)

        def mergeCombiner(x, y):
            return y

        rddOfLast = self._sortIfNeeded(self._prereducedrdd.combineByKey(
            createCombiner,
            mergeValue,
            mergeCombiner)).values()
        return PRDD.fromRDD(rddOfLast)

    def _regroup_groupedrdd(self):
        """
        A common pattern is we want to call groupby again on the dataframes
        so we can use the groupby functions.
        """
        myargs = self._myargs
        mykwargs = self._mykwargs

        def regroup(df):
            return df.groupby(*myargs, **mykwargs)
        return self._groupedrdd.mapValues(regroup)

    def nth(self, n, dropna=None):
        """
        Take the nth element of each grouby.
        """
        # TODO: Stop collecting the entire frame for each key.
        myargs = self._myargs
        mykwargs = self._mykwargs
        nthRDD = self._regroup_groupedrdd().mapValues(
            lambda r: r.nth(
                n,
                dropna=dropna)).values()
        return PRDD.fromRDD(nthRDD)

    def aggregate(self, f):
        """
        Apply the aggregation function.
        Note: This implementation does note take advantage of partail
        aggregation.
        """
        return PRDD.fromRDD(
            self._regroup_groupedrdd().values().map(
                lambda g: g.aggregate(f)))

    def agg(self, f):
        return self.aggregate(f)

    def apply(self, func, *args, **kwargs):
        """
        Apply the provided function and combine the results together in the
        same way as apply from groupby in pandas.

        This returns a PRDD.
        """
        def key_by_index(data):
            """
            Key each row by its index
            """
            # TODO: Is there a better way to do this?
            for key, row in data.iterrows():
                yield (key, pandas.DataFrame.from_dict(dict([(key, row)]),
                                                       orient='index'))

        myargs = self._myargs
        mykwargs = self._mykwargs
        regroupedRDD = self._prereducedrdd.mapValues(
            lambda data: data.groupby(*myargs, **mykwargs))
        appliedRDD = regroupedRDD.map(
            lambda key_data: key_data[1].apply(func, *args, **kwargs))
        reKeyedRDD = appliedRDD.flatMap(key_by_index)
        prdd = self._sortIfNeeded(reKeyedRDD).values()
        return PRDD.fromRDD(prdd)
