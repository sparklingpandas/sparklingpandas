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
from sparklingpandas.pstats import PStats
from functools import reduce
from itertools import chain, imap

add_pyspark_path()
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup
from pyspark.rdd import RDD
import pyspark
import pandas



class PRDD:

    """A Panda Resilient Distributed Dataset (PRDD), is based on
    Spark SQL's DataFrame (previously known as SchemaRDD). PRDDs aim to
    provide, as close as reasonable, Panda's compatable inferface.
    Note: RDDs are lazy, so you operations are not performed until required."""

    def __init__(self, schema_rdd):
        self._schema_rdd = schema_rdd
        self._sqlCtx = schema_rdd.sql_ctx

    def _rdd(self):
        """Return an RDD of Panda DataFrame objects. This can be expensive
        especially if we don't do a narrow transformation after and get it back
        to Spark SQL land quickly."""
        columns = self._schema_rdd.columns

        def fromRecords(records):
            if not records:
                return []
            else:
                return [pandas.DataFrame.from_records([records], columns=columns)]

        return self._schema_rdd.rdd.flatMap(fromRecords)

    def __evil_apply_with_dataframes(self, func):
        """Convert the underlying SchmeaRDD to an RDD of DataFrames.
        apply the provide function and convert the result back.
        This is hella slow."""
        source_rdd = self._rdd()
        result_rdd = func(source_rdd)
        return self.from_rdd_of_dataframes(result_rdd)

    def from_rdd_of_dataframes(self, rdd):
        """Take an RDD of dataframes and return a PRDD"""
        def frame_to_spark_sql(frame):
            """Convert a Panda's DataFrame into Spark SQL Rows"""
            # TODO: Convert to row objects directly?
            return map((lambda x: x[1].to_dict()), frame.iterrows())
        return PRDD.fromSchemaRDD(self._sqlCtx.inferSchema(rdd.flatMap(frame_to_spark_sql)))

    @classmethod
    def fromSchemaRDD(cls, schemaRdd):
        """Construct a PRDD from an SchemaRDD.
        No checking or validation occurs."""
        return PRDD(schemaRdd)

    @classmethod
    def fromDataFrameRDD(cls, rdd):
        """Construct a PRDD from an RDD of DataFrames.
        No checking or validation occurs."""
        result = PRDD(None)
        return result.from_rdd_of_dataframes(rdd)

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
        return self.from_rdd_of_dataframes(
            self._rdd().map(lambda data: data.applymap(f), **kwargs))

    def __getitem__(self, key):
        """Returns a new PRDD of elements from that key."""
        return self.from_spark_df(self._schema_rdd.select(key))

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False):
        """Returns a groupby on the schema rdd. This returns a GroupBy object.
        Note that grouping by a column name will be faster than most other
        options due to implementation."""
        from sparklingpandas.groupby import GroupBy
        return GroupBy(self, by=by, axis=axis, level=level, as_index=as_index,
                       sort=sort, group_keys=group_keys, squeeze=squeeze)

    def _first_as_df(self):
        """Gets the first row as a dataframe. Useful for functions like
        dtypas & ftypes"""
        return pandas.DataFrame.from_records(
            [self._schema_rdd.first()],
            columns=self._schema_rdd.columns)

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
        return self._first_as_df().ftypes

    def get_dtype_counts(self):
        """
        Return the counts of dtypes in this object
        Uses the information from the first frame
        """
        return self._first_as_df().get_dtype_counts()

    def get_ftype_counts(self):
        """
        Return the counts of ftypes in this object
        Uses the information from the first frame
        """
        return self._first_as_df().get_ftype_counts()

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
        return self._schema_rdd.toPandas().reset_index()

    def stats(self, columns):
        """Compute the stats for each column provided in columns.
        Parameters
        ----------
        columns : list of str, contains all columns to compute stats on.
        """
        assert (not isinstance(columns, basestring)), "columns should be a " \
                                                      "list of strs,  " \
                                                      "not a str!"
        assert isinstance(columns, list), "columns should be a list!"

        from pyspark.sql import functions as F
        functions = [F.min, F.max, F.avg, F.count]
        aggs = list(self._flatmap(lambda column: map(lambda f: f(column), functions),columns))
        return PStats(self.fromSchemaRDD(self._schema_rdd.agg(*aggs)))

    def min(self):
        return self.from_spark_df(prdd._schema_rdd.min())

    def max(self):
        return self.from_spark_df(prdd._schema_rdd.max())

    def avg(self):
        return self.from_spark_df(prdd._schema_rdd.avg())

    def _flatmap(self, f, items):
        return chain.from_iterable(imap(f, items))
