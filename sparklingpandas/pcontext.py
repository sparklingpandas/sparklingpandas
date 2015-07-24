"""Provide an easy interface for loading data into L{Dataframe}s for Spark.
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

add_pyspark_path()
import pandas
from StringIO import StringIO as sio
from pyspark.context import SparkContext
from sparklingpandas.dataframe import Dataframe, _normalize_index_names
from sparklingpandas.custom_functions import register_sql_extensions


class PSparkContext():

    """This is a thin wrapper around SparkContext from PySpark which makes it
    easy to load data into L{PRDD}s."""

    def __init__(self, spark_context, sql_ctx=None):
        """Initialize a PSparkContext with the associacted spark context,
        and spark sql context if provided.
        :param spark_context: Initialized and configured spark context.
        :param sql_ctx: Initialized and configured SQL context, if relevant.
        :return: Correctly initialized SparklingPandasContext.
        """
        self.spark_ctx = spark_context
        if sql_ctx:
            self.sql_ctx = sql_ctx
        else:
            from pyspark.sql import SQLContext
            self.sql_ctx = SQLContext(self.spark_ctx)
        # Register our magical functions
        register_sql_extensions(self.sql_ctx)

    @classmethod
    def simple(cls, *args, **kwargs):
        """Takes the same arguments as SparkContext and constructs a
        PSparkContext"""
        return PSparkContext(SparkContext(*args, **kwargs))

    def read_csv(self, name, use_whole_file=False, names=None, skiprows=0,
                 *args, **kwargs):
        """Read a CSV file in and parse it into Pandas DataFrames.
        If no names is provided we use the first row for the names.
        header=0 is the default unless names is provided in which case
        header=None is the default.
        skiprows indicates how many rows of input to skip. This will
        only be applied to the first partition of the data (so if
        #skiprows > #row in first partition this will not work). Generally
        this shouldn't be an issue for small values of skiprows.
        No other values of header is supported.
        All additional parameters are passed to the read_csv function.
        TODO: Use spark-csv package if the request could be fulfilled by it.
        """
        def csv_file(partition_number, files):
            # pylint: disable=unexpected-keyword-arg
            file_count = 0
            for _, contents in files:
                # Only skip lines on the first file
                if partition_number == 0 and file_count == 0 and _skiprows > 0:
                    yield pandas.read_csv(
                        sio(contents), *args,
                        header=None,
                        names=mynames,
                        skiprows=_skiprows,
                        **kwargs)
                else:
                    file_count += 1
                    yield pandas.read_csv(
                        sio(contents), *args,
                        header=None,
                        names=mynames,
                        **kwargs)

        def csv_rows(partition_number, rows):
            # pylint: disable=unexpected-keyword-arg
            in_str = "\n".join(rows)
            if partition_number == 0:
                return iter([
                    pandas.read_csv(
                        sio(in_str), *args, header=None,
                        names=mynames,
                        skiprows=_skiprows,
                        **kwargs)])
            else:
                # could use .iterows instead?
                return iter([pandas.read_csv(sio(in_str), *args, header=None,
                                             names=mynames, **kwargs)])

        # If we need to peak at the first partition and determine the column
        # names
        mynames = None
        _skiprows = skiprows
        if names:
            mynames = names
        else:
            # In the future we could avoid this expensive call.
            first_line = self.spark_ctx.textFile(name).first()
            frame = pandas.read_csv(sio(first_line), **kwargs)
            # pylint sees frame as a tuple despite it being a Dataframe
            mynames = list(frame.columns)
            _skiprows += 1

        # Do the actual load
        if use_whole_file:
            return self.from_pandas_rdd(
                self.spark_ctx.wholeTextFiles(name)
                .mapPartitionsWithIndex(csv_file))
        else:
            return self.from_pandas_rdd(
                self.spark_ctx.textFile(name).mapPartitionsWithIndex(csv_rows))

    def parquetFile(self, *paths):
        """Loads a Parquet file, returning the result as a L{Dataframe}.
        """
        return self.from_spark_rdd(self.sql_ctx.parquetFile(paths),
                                   self.sql_ctx)

    def jsonFile(self, path, schema=None, sampling_ratio=1.0):
        """Loads a text file storing one JSON object per line as a
        L{Dataframe}.
        """
        schema_rdd = self.sql_ctx.jsonFile(path, schema, sampling_ratio)
        return self.from_spark_rdd(schema_rdd, self.sql_ctx)

    def from_pd_data_frame(self, local_df):
        """Make a distributed dataframe from a local dataframe. The intend use
        is for testing. Note: dtypes are re-infered, so they may not match."""
        def frame_to_rows(frame):
            """Convert a Panda's DataFrame into Spark SQL Rows"""
            # TODO: Convert to row objects directly?
            return [r.tolist() for r in frame.to_records()]
        schema = list(local_df.columns)
        index_names = list(local_df.index.names)
        index_names = _normalize_index_names(index_names)
        schema = index_names + schema
        rows = self.spark_ctx.parallelize(frame_to_rows(local_df))
        sp_df = Dataframe.from_schema_rdd(
            self.sql_ctx.createDataFrame(
                rows,
                schema=schema,
                # Look at all the rows, should be ok since coming from
                # a local dataset
                samplingRatio=1))
        sp_df._index_names = index_names
        return sp_df

    def sql(self, query):
        """Perform a SQL query and create a L{Dataframe} of the result."""
        return Dataframe.from_spark_rdd(self.sql_ctx.sql(query), self.sql_ctx)

    def table(self, table):
        """Returns the provided table as a L{Dataframe}"""
        return Dataframe.from_spark_rdd(self.sql_ctx.table(table),
                                        self.sql_ctx)

    def from_spark_rdd(self, spark_rdd, sql_ctx):
        """
        Translates a Spark DataFrame Rdd into a SparklingPandas dataframe.
        :param dataframe_rdd: Input dataframe RDD to convert
        :return: Matchign SparklingPandas dataframe
        """
        return Dataframe.from_spark_rdd(spark_rdd, sql_ctx)

    def DataFrame(self, elements, *args, **kwargs):
        """Wraps the pandas.DataFrame operation."""
        return self.from_pd_data_frame(pandas.DataFrame(
            elements,
            *args,
            **kwargs))

    def from_pandas_rdd(self, pandas_rdd):
        def _extract_records(data):
            return [r for r in data.to_records(index=False).tolist()]

        def _from_pandas_rdd_records(pandas_rdd_records, schema):
            """Create a L{Dataframe} from an RDD of records with schema"""
            return Dataframe.from_spark_rdd(
                self.sql_ctx.createDataFrame(pandas_rdd_records,
                                             schema.values.tolist()),
                self.sql_ctx)

        schema = pandas_rdd.map(lambda x: x.columns).first()
        rdd_records = pandas_rdd.flatMap(_extract_records)
        return _from_pandas_rdd_records(rdd_records, schema)

    def read_json(self, name,
                  *args, **kwargs):
        """Read a json file in and parse it into Pandas DataFrames.
        If no names is provided we use the first row for the names.
        Currently, it is not possible to skip the first n rows of a file.
        Headers are provided in the json file and not specified separately.
        """
        def json_file_to_df(files):
            """ Transforms a JSON file into a list of data"""
            for _, contents in files:
                yield pandas.read_json(sio(contents), *args, **kwargs)

        return self.from_pandas_rdd(self.spark_ctx.wholeTextFiles(name)
                                    .mapPartitions(json_file_to_df))

    def stop(self):
        """Stop the underlying SparkContext
        """
        self.spark_ctx.stop()
