"""Provide an easy interface for loading data into L{PRDD}s for Spark.
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
from sparklingpandas.prdd import PRDD


class PSparkContext():

    """This is a thin wrapper around SparkContext from PySpark which makes it
    easy to load data into L{PRDD}s."""

    def __init__(self, sparkcontext, sqlCtx=None):
        self.sc = sparkcontext
        if sqlCtx:
            self.sql_ctx = sqlCtx
        else:
            from pyspark.sql import SQLContext
            self.sql_ctx = SQLContext(self.sc)

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
        TODO: Use spark-csv package if the request could be fufilled by it.
        """
        def csv_file(partitionNumber, files):
            file_count = 0
            for filename, contents in files:
                # Only skip lines on the first file
                if partitionNumber == 0 and file_count == 0 and _skiprows > 0:
                    yield pandas.read_csv(sio(contents), *args,
                                          header=None,
                                          names=mynames,
                                          skiprows=_skiprows, **kwargs)
                else:
                    file_count += 1
                    yield pandas.read_csv(sio(contents), *args,
                                          header=None,
                                          names=mynames,
                                          **kwargs)

        def csv_rows(partitionNumber, rows):
            rowCount = 0
            inputStr = "\n".join(rows)
            if partitionNumber == 0:
                return iter([pandas.read_csv(sio(inputStr), *args, header=None,
                                             names=mynames, skiprows=_skiprows,
                                             **kwargs)])
            else:
                # could use .iterows instead?
                return iter([pandas.read_csv(sio(inputStr), *args, header=None,
                                             names=mynames, **kwargs)])

        # If we need to peak at the first partition and determine the column
        # names
        mynames = None
        _skiprows = skiprows
        if names:
            mynames = names
        else:
            # In the future we could avoid this expensive call.
            first_line = self.sc.textFile(name).first()
            frame = pandas.read_csv(sio(first_line), **kwargs)
            mynames = list(frame.columns.values)
            _skiprows += 1

        # Do the actual load
        if use_whole_file:
            return self.from_pandas_RDD(
                self.sc.wholeTextFiles(name).mapPartitionsWithIndex(csv_file))
        else:
            return self.from_pandas_RDD(
                self.sc.textFile(name).mapPartitionsWithIndex(csv_rows))

    def parquetFile(self, *paths):
        """Loads a Parquet file, returning the result as a L{PRDD}.
        """
        return self.from_spark_df(self.sql_ctx.parquetFile(paths))

    def jsonFile(self, path, schema=None, samplingRatio=1.0):
        """Loads a text file storing one JSON object per line as a
        L{PRDD}.
        """
        schema_rdd = self.sql_ctx.jsonFile(path, schema, samplingRatio)
        return self.from_spark_df(schema_rdd)

    def from_data_frame(self, df):
        return PRDD.from_spark_df(self.sql_ctx.createDataFrame(df))

    def sql(self, query):
        """Perform a SQL query and create a L{PRDD} of the result."""
        return PRDD.from_spark_df(self.sql_ctx.sql(query))

    def table(self, table):
        """Returns the provided table as a L{PRDD}"""
        return PRDD.from_spark_df(self.sql_ctx.table(query))

    def from_schema_rdd(self, schemaRDD):
        return PRDD.from_spark_df(schemaRDD)

    def from_spark_df(self, schemaRDD):
        return PRDD.from_spark_df(schemaRDD)

    def DataFrame(self, elements, *args, **kwargs):
        """Wraps the pandas.DataFrame operation."""
        return self.from_schema_rdd(
            self.sql_ctx.createDataFrame(pandas.DataFrame(
                elements,
                *args,
                **kwargs)))

    def from_pandas_rdd(self, pandas_rdd):
        def _extract_records(data):
            return [r.toList() for r in data.to_records(index=False)]

        def _from_pandas_rdd_records(pandas_rdd_records, schema):
            """Createa a L{PRDD} from an RDD of records with schema"""
            return PRDD.from_spark_df(
                self.sql_ctx.createDataFrame(pandas_rdd_records, schema))

        schema = pandas_rdd.map(lambda x: x.columns).first
        rdd_records = pandas_rdd.flatMap(_extract_records)
        return _from_pandas_rdd_records(rdd_records, schema)

    def stop(self):
        """Stop the underlying SparkContext
        """
        self.sc.stop()
