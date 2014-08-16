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
from StringIO import StringIO
from pyspark.context import SparkContext
from sparklingpandas.prdd import PRDD


class PSparkContext():

    """This is a thin wrapper around SparkContext from PySpark which makes it
    easy to load data into L{PRDD}s."""

    def __init__(self, sparkcontext, sqlcontext=None, hivecontext=None):
        self.sc = sparkcontext
        self.sql_ctx = sqlcontext
        self.hive_ctx = hivecontext

    def _get_sql_ctx(self):
        """Return the sql context or construct it if needed."""
        if self.sql_ctx:
            return self.sql_ctx
        else:
            from pyspark.sql import SQLContext
            self.sql_ctx = SQLContext(self.sc)
            return self.sql_ctx

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
        """
        def csv_file(partitionNumber, files):
            file_count = 0
            for filename, contents in files:
                # Only skip lines on the first file
                if partitionNumber == 0 and file_count == 0 and _skiprows > 0:
                    yield pandas.read_csv(StringIO(contents), *args,
                                          header=None,
                                          names=mynames,
                                          skiprows=_skiprows, **kwargs)
                else:
                    file_count += 1
                    yield pandas.read_csv(StringIO(contents), *args,
                                          header=None,
                                          names=mynames,
                                          **kwargs)

        def csv_rows(partitionNumber, rows):
            rowCount = 0
            inputStr = "\n".join(rows)
            if partitionNumber == 0:
                return pandas.read_csv(StringIO(row), *args, header=None,
                                       names=mynames, skiprows=_skiprows,
                                       **kwargs)
            else:
                return pandas.read_csv(StringIO(row), *args, header=None,
                                       names=mynames, **kwargs)

        # If we need to peak at the first partition and determine the column
        # names
        mynames = None
        _skiprows = skiprows
        if names:
            mynames = names
        else:
            # In the future we could avoid this expensive call.
            first_line = self.sc.textFile(name).first()
            frame = pandas.read_csv(StringIO(first_line))
            mynames = list(frame.columns.values)
            _skiprows += 1

        # Do the actual load
        if use_whole_file:
            return PRDD.fromRDD(
                self.sc.wholeTextFiles(name).mapPartitionsWithIndex(csv_file))
        else:
            return PRDD.fromRDD(
                self.sc.textFile(name).mapPartitionsWithIndex(csv_rows))

    def from_data_frame(self, df):
        """Make a distributed dataframe from a local dataframe. The intend use
        is for testing. Note: dtypes are re-infered, so they may not match."""
        mydtype = df.dtypes
        mycols = df.columns

        def loadFromKeyRow(partition):
            pll = list(partition)
            if len(pll) > 0:
                index, data = zip(*pll)
                return iter([
                    pandas.DataFrame(
                        list(data),
                        columns=mycols,
                        index=index)])
            else:
                return iter([])
        indexedData = zip(df.index, df.itertuples(index=False))
        rdd = self.sc.parallelize(indexedData).mapPartitions(loadFromKeyRow)
        return PRDD.fromRDD(rdd)

    def sql(self, query):
        """Perform a SQL query and create a L{PRDD} of the result."""
        return PRDD.fromRDD(
            self.from_schema_rdd(
                self._get_sqlctx().sql(query)))

    def from_schema_rdd(self, schemaRDD):
        """Convert a schema RDD to a L{PRDD}."""
        def _load_kv_partitions(partition):
            """Convert a partition where each row is key/value data."""
            partitionList = list(partition)
            if len(partitionList) > 0:
                return iter([
                    pandas.DataFrame(data=partitionList)
                ])
            else:
                return iter([])
        return PRDD.fromRDD(schemaRDD.mapPartitions(_load_kv_partitions))

    def DataFrame(self, elements, *args, **kwargs):
        """Wraps the pandas.DataFrame operation."""
        def _load_partitions(partition):
            """Convert partitions of tuples."""
            partitionList = list(partition)
            if len(partitionList) > 0:
                (indices, elements) = zip(*partitionList)
                return iter([
                    pandas.DataFrame(
                        data=list(elements),
                        index=list(indices),
                        *args,
                        **kwargs)])
            else:
                return iter([])
        # Zip with the index so we have consistent indexing as if it was
        # operated on locally
        index = range(len(elements))
        # TODO(holden): test this issue #13
        if 'index' in kwargs:
            index = kwargs['index']
        elementsWithIndex = zip(index, elements)
        return PRDD.fromRDD(
            self.sc.parallelize(elementsWithIndex).mapPartitions(
                _load_partitions))

    def stop(self):
        """Stop the underlying SparkContext
        """
        self.sc.stop()
