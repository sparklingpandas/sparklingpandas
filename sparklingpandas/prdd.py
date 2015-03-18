"""Provide a way to work with panda data frames in Spark"""
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
from functools import reduce

add_pyspark_path()
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup
from pyspark.rdd import RDD
from sparklingpandas.pstatcounter import PStatCounter
import pandas


class PRDD:

    """A Panda Resilient Distributed Dataset (PRDD), is an extension of the SchemaRDD.
    Note: RDDs are lazy, so you operations are not performed until required."""

    def __init__(self, schema_rdd):
        self._schema_rdd = schema_rdd

    def _rdd(self):
        """Return an RDD of Panda DataFrame objects"""
        columns = self._schema_rdd.columns
        def fromRecords(records):
            return [pandas.DataFrame.from_records(records, columns=columns)]
        return self._schema_rdd.rdd.flatMap(fromRecords)

    @classmethod
    def from_spark_df(cls, schema_rdd):
        """Construct a PRDD from an RDD. No checking or validation occurs."""
        return PRDD(schema_rdd)

    def to_spark_sql(self):
        """A Sparkling Pandas specific function to turn a DDF into
        something that Spark SQL can query. To use the result you will
        need to call sqlCtx.inferSchema(rdd) and then register the result
        as a table. Once Spark 1.1 is released this function may be deprecated
        and replacted with to_spark_sql_schema_rdd."""
        return self._schema_rdd

    def applymap(self, f, **kwargs):
        """Return a new PRDD by applying a function to each element of each
        Panda DataFrame."""
        return self.fromRDD(
            self._rdd.map(lambda data: data.applymap(f), **kwargs))

    def __getitem__(self, key):
        """Returns a new PRDD of elements from that key."""
        return self.from_spark_df(self._schema_rdd[key])

    def groupby(self, *args, **kwargs):
        """Takes the same parameters as groupby on DataFrame.
        Like with groupby on DataFrame disabling sorting will result in an
        even larger performance improvement. This returns a Sparkling Pandas
        L{GroupBy} object which supports many of the same operations as regular
        GroupBy but not all."""
        return self.from_spark_df(self._schema_rdd.groupBy(*args))

    def _first_as_df(self):
        """Gets the first row as a dataframe. Useful for functions like dtypas & ftypes"""
        return pandas.from_records([self._schema_rdd.first()], columns = self._schema_rdd.columns)

    @property
    def dtypes(self):
        """
        Return the dtypes associated with this object
        Uses the types from the first frame.
        """
        return self._first_as_df().dtypes

    @property
    def ftypes(self):
        """
        Return the ftypes associated with this object
        Uses the types from the first frame.
        """
        return self._first_as_df.dtypes

    def get_dtype_counts(self):
        """
        Return the counts of dtypes in this object
        Uses the information from the first frame
        """
        return self._first_as_df.get_dtype_counts()

    def get_ftype_counts(self):
        """
        Return the counts of ftypes in this object
        Uses the information from the first frame
        """
        return self._first_as_df.get_ftype_counts()

    @property
    def axes(self):
        return (self._rdd().map(lambda frame: frame.axes)
                .reduce(lambda xy, ab: [xy[0].append(ab[0]), xy[1]]))

    @property
    def shape(self):
        return (self._rdd().map(lambda frame: frame.shape)
                .reduce(lambda xy, ab: (xy[0] + ab[0], xy[1])))

    def collect(self):
        """Collect the elements in an PRDD and concatenate the partition."""
        return self._schema_rdd.toPandas()

    def stats(self, columns):
        """Compute the stats for each column provided in columns.
        Parameters
        ----------
        columns : list of str, contains all columns to compute stats on.
        """
        def reduceFunc(sc1, sc2):
            return sc1.merge_pstats(sc2)

        return self._rdd.mapPartitions(
            lambda i: [PStatCounter(dataframes=i, columns=columns)]).reduce(
            reduceFunc)

        def reduce_func(sc1, sc2):
            return sc1.merge_pstats(sc2)

        return self._rdd.mapPartitions(
            lambda i: [PStatCounter(dataframes=i, columns=columns)]).reduce(
                reduce_func)
