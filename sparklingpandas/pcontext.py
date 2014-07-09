"""
Provide an easy interface for loading data into L{PRDD}s for Spark.
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

    """
    This is a thin wrapper around SparkContext from
    PySpark which makes it easy to load data into L{PRDD}s.
    """

    def __init__(self, sparkcontext):
        self.sc = sparkcontext

    @classmethod
    def simple(cls, *args, **kwargs):
        """
        Takes the same arguments as SparkContext and constructs a PSparkContext
        """
        return PSparkContext(SparkContext(*args, **kwargs))

    def csvfile(self, name, use_whole_file=True, *args, **kwargs):
        """
        Read a CSV file in and parse it into panda data frames. Note this uses
        wholeTextFiles by default underneath the hood so as to support
        multi-line CSV records so many small input files are preferred.
        All additional parameters are passed to the read_csv function
        """
        # TODO(holden): Figure out what the deal with indexing will be for this
        # issue #12
        def csv_file(contents, *args, **kwargs):
            return pandas.read_csv(StringIO(contents), *args, header=0,
                                   **kwargs)

        def csv_rows(rows, *args, **kwargs):
            for row in rows:
                yield pandas.read_csv(StringIO(row), *args, header=0, **kwargs)

        if use_whole_file:
            return PRDD.fromRDD(
                self.sc.wholeTextFiles(name).map(
                    lambda
                    name_contents:
                    csv_file(name_contents[1],
                             *args, **kwargs)))
        else:
            return PRDD.fromRDD(self.sc.textFile(name).mapPartitions(
                lambda x: csv_rows(x, *args, **kwargs)))

    def from_data_frame(self, df):
        """
        Make a distributed dataframe from a local dataframe. Intend use is
        mostly for testing. Note: dtypes are re-infered, so they may not match.
        """
        mydtype = df.dtypes
        mycols = df.columns

        def loadFromKeyRow(partition):
            pll = list(partition)
            if len(pll) > 0:
                index, data = zip(*pll)
                return [
                    pandas.DataFrame(
                        list(data),
                        columns=mycols,
                        index=index)]
            else:
                return []
        indexedData = zip(df.index, df.itertuples(index=False))
        rdd = self.sc.parallelize(indexedData).mapPartitions(loadFromKeyRow)
        return PRDD.fromRDD(rdd)

    def DataFrame(self, elements, *args, **kwargs):
        """
        Wraps the pandas.DataFrame operation.
        """
        def loadPartitions(partition):
            partitionList = list(partition)
            if len(partitionList) > 0:
                (indices, elements) = zip(*partitionList)
                return [
                    pandas.DataFrame(
                        data=list(elements),
                        index=list(indices),
                        *args,
                        **kwargs)]
            else:
                return []
        # Zip with the index so we have consistent indexing as if it was
        # operated on locally
        index = range(len(elements))
        # TODO(holden): test this issue #13
        if 'index' in kwargs:
            index = kwargs['index']
        elementsWithIndex = zip(index, elements)
        return PRDD.fromRDD(
            self.sc.parallelize(elementsWithIndex).mapPartitions(
                loadPartitions))

    def stop(self):
        """
        Stop the underlying SparkContext
        """
        self.sc.stop()
