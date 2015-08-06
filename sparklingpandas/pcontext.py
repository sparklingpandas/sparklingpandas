"""Provide an easy interface for loading data into L{DataFrame}s for Spark.
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
from sparklingpandas.dataframe import DataFrame, _normalize_index_names
import logging


class PSparkContext():
    """This is a thin wrapper around SparkContext from PySpark which makes it
    easy to load data into L{DataFrame}s."""

    def __init__(self, spark_context, sql_ctx=None):
        """Initialize a PSparkContext with the associacted spark context,
        and Spark SQL context if provided. This context is usef to load
        data into L{DataFrame}s.

        Parameters
        ----------
        spark_context: SparkContext
            Initialized and configured spark context. If you are running in the
            PySpark shell, this is already created as "sc".
        sql_ctx: SQLContext, optional
            Initialized and configured SQL context, if not provided Sparkling
            Panda's will create one.
        Returns
        -------
        Correctly initialized SparklingPandasContext.
        """
        self.spark_ctx = spark_context
        if sql_ctx:
            self.sql_ctx = sql_ctx
        else:
            logging.info("No sql context provided, creating")
            from pyspark.sql import SQLContext
            self.sql_ctx = SQLContext(self.spark_ctx)

    @classmethod
    def simple(cls, *args, **kwargs):
        """Takes the same arguments as SparkContext and constructs a
        PSparkContext"""
        return PSparkContext(SparkContext(*args, **kwargs))

    def read_csv(self, file_path, use_whole_file=False, names=None, skiprows=0,
                 *args, **kwargs):
        """Read a CSV file in and parse it into Pandas DataFrames. By default,
        the first row from the first partition of that data is parsed and used
        as the column names for the data from. If no 'names' param is
        provided we parse the first row of the first partition of data and
        use it for column names.

        Parameters
        ----------
        file_path: string
            Path to input. Any valid file path in Spark works here, eg:
            'file:///my/path/in/local/file/system' or 'hdfs:/user/juliet/'
        use_whole_file: boolean
            Whether of not to use the whole file.
        names: list of strings, optional
        skiprows: integer, optional
            indicates how many rows of input to skip. This will
            only be applied to the first partition of the data (so if
            #skiprows > #row in first partition this will not work). Generally
            this shouldn't be an issue for small values of skiprows.
        No other value of header is supported.
        All additional parameters available in pandas.read_csv() are usable
        here.

        Returns
        -------
        A SparklingPandas DataFrame that contains the data from the
        specified file.
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
            first_line = self.spark_ctx.textFile(file_path).first()
            frame = pandas.read_csv(sio(first_line), **kwargs)
            # pylint sees frame as a tuple despite it being a DataFrame
            mynames = list(frame.columns)
            _skiprows += 1

        # Do the actual load
        if use_whole_file:
            return self.from_pandas_rdd(
                self.spark_ctx.wholeTextFiles(file_path)
                .mapPartitionsWithIndex(csv_file))
        else:
            return self.from_pandas_rdd(
                self.spark_ctx.textFile(file_path)
                    .mapPartitionsWithIndex(csv_rows))

    def parquetFile(self, *paths):
        """Loads a Parquet file, returning the result as a L{DataFrame}.

        Parameters
        ----------
        paths: string, variable length
             The path(s) of the parquet files to load. Should be Hadoop style
             paths (e.g. hdfs://..., file://... etc.).
        Returns
        -------
        A L{DataFrame} of the contents of the parquet files.
        """
        return self.from_spark_rdd(self.sql_ctx.parquetFile(paths))

    def jsonFile(self, path, schema=None, sampling_ratio=1.0):
        """Loads a text file storing one JSON object per line as a
        L{DataFrame}.
        Parameters
        ----------
        path: string
             The path of the json files to load. Should be Hadoop style
             paths (e.g. hdfs://..., file://... etc.).
        schema: StructType, optional
             If you know the schema of your input data you can specify it. The
             schema is specified using Spark SQL's schema format. If not
             specified will sample the json records to determine the schema.
             Spark SQL's schema format is documented (somewhat) in the
             "Programmatically Specifying the Schema" of the Spark SQL
             programming guide at: http://bit.ly/sparkSQLprogrammingGuide
        sampling_ratio: int, default=1.0
             Percentage of the records to sample when infering schema.
             Defaults to all records for safety, but you may be able to set to
             a lower ratio if the same fields are present accross records or
             your input is of sufficient size.
        Returns
        -------
        A L{DataFrame} of the contents of the json files.
        """
        schema_rdd = self.sql_ctx.jsonFile(path, schema, sampling_ratio)
        return self.from_spark_rdd(schema_rdd)

    def from_pd_data_frame(self, local_df):
        """Make a Sparkling Pandas dataframe from a local Pandas DataFrame.
        The intend use is for testing or joining distributed data with local
        data.
        The types are re-infered, so they may not match.
        Parameters
        ----------
        local_df: Pandas DataFrame
            The data to turn into a distributed Sparkling Pandas DataFrame.
            See http://bit.ly/pandasDataFrame for docs.
        Returns
        -------
        A Sparkling Pandas DataFrame.
        """
        def frame_to_rows(frame):
            """Convert a Pandas DataFrame into a list of Spark SQL Rows"""
            # TODO: Convert to row objects directly?
            return [r.tolist() for r in frame.to_records()]
        schema = list(local_df.columns)
        index_names = list(local_df.index.names)
        index_names = _normalize_index_names(index_names)
        schema = index_names + schema
        rows = self.spark_ctx.parallelize(frame_to_rows(local_df))
        sp_df = DataFrame.from_schema_rdd(
            self.sql_ctx.createDataFrame(
                rows,
                schema=schema,
                # Look at all the rows, should be ok since coming from
                # a local dataset
                samplingRatio=1))
        sp_df._index_names = index_names
        return sp_df

    def sql(self, query):
        """Perform a SQL query and create a L{DataFrame} of the result.
        The SQL query is run using Spark SQL. This is not intended for
        querying arbitrary databases, but rather querying Spark SQL tables.
        Parameters
        ----------
        query: string
            The SQL query to pass to Spark SQL to execute.
        Returns
        -------
        Sparkling Pandas DataFrame.
        """
        return DataFrame.from_spark_rdd(self.sql_ctx.sql(query), self.sql_ctx)

    def table(self, table):
        """Returns the provided Spark SQL table as a L{DataFrame}
        Parameters
        ----------
        table: string
            The name of the Spark SQL table to turn into a L{DataFrame}
        Returns
        -------
        Sparkling Pandas DataFrame.
        """
        return DataFrame.from_spark_rdd(self.sql_ctx.table(table),
                                        self.sql_ctx)

    def from_spark_rdd(self, spark_rdd):
        """
        Translates a Spark DataFrame into a Sparkling Pandas Dataframe.
        Currently, no checking or validation occurs.
        Parameters
        ----------
        spark_rdd: Spark DataFrame
            Input Spark DataFrame.
        Returns
        -------
        Sparkling Pandas DataFrame.
        """
        return DataFrame.from_spark_rdd(spark_rdd, self.sql_ctx)

    def DataFrame(self, elements, *args, **kwargs):
        """Create a Sparkling Pandas DataFrame for the provided
        elements, following the same API as constructing a Panda's DataFrame.
        Note: since elements is local this is only useful for distributing
        dataframes which are small enough to fit on a single machine anyways.
        Parameters
        ----------
        elements: numpy ndarray (structured or homogeneous), dict, or
        Pandas DataFrame.
            Input elements to use with the DataFrame.
        Additional parameters as defined by L{pandas.DataFrame}.
        Returns
        -------
        Sparkling Pandas DataFrame."""
        return self.from_pd_data_frame(pandas.DataFrame(
            elements,
            *args,
            **kwargs))

    def from_pandas_rdd(self, pandas_rdd):
        """Create a Sparkling Pandas DataFrame from the provided RDD
        which is comprised of Panda's DataFrame. Note: the current version
        drops index information.
        Parameters
        ----------
        pandas_rdd: RDD[pandas.DataFrame]
        Returns
        -------
        Sparkling Pandas DataFrame."""
        return DataFrame.fromDataFrameRDD(pandas_rdd, self.sql_ctx)

    def read_json(self, file_path,
                  *args, **kwargs):
        """Read a json file in and parse it into Pandas DataFrames.
        If no names is provided we use the first row for the names.
        Currently, it is not possible to skip the first n rows of a file.
        Headers are provided in the json file and not specified separately.

        Parameters
        ----------
        file_path: string
            Path to input. Any valid file path in Spark works here, eg:
            'my/path/in/local/file/system' or 'hdfs:/user/juliet/'
        Other than skipRows, all additional parameters available in
        pandas.read_csv() are usable here.

        Returns
        -------
            A SparklingPandas DataFrame that contains the data from the
        specified file.
        """
        def json_file_to_df(files):
            """ Transforms a JSON file into a list of data"""
            for _, contents in files:
                yield pandas.read_json(sio(contents), *args, **kwargs)

        return self.from_pandas_rdd(self.spark_ctx.wholeTextFiles(file_path)
                                    .mapPartitions(json_file_to_df))

    def stop(self):
        """Stop the underlying SparkContext
        """
        self.spark_ctx.stop()
