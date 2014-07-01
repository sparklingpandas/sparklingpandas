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
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pandaspark.utils import add_pyspark_path, run_tests
add_pyspark_path()
import pandas
from StringIO import StringIO
from pyspark.context import SparkContext
from pandaspark.prdd import PRDD

class PSparkContext():
    """
    This is a thin wrapper around SparkContext from
    PySpark which makes it easy to load data into L{PRDD}s.
    """

    def __init__(self, sparkcontext):
        self.sc = sparkcontext

    @classmethod
    def simple(cls, *args, **kwargs):
        """Takes the same arguments as SparkContext and constructs a PSparkContext"""
        return PSparkContext(SparkContext(*args, **kwargs))

    def csvfile(self, name, useWholeFile=True, *args, **kwargs):
        """
        Read a CSV file in and parse it into panda data frames. Note this uses
        wholeTextFiles by default underneath the hood so as to support
        multi-line CSV records so many small input files are prefered.
        All additional parameters are passed to the read_csv function
        """
        # TODO(holden): string IO stuff
        def csv_file(contents, *args, **kwargs):
            return pandas.read_csv(StringIO(contents), *args, header=0,**kwargs)
        def csv_rows(rows, *args, **kwargs):
            for row in rows:
                yield pandas.read_csv(StringIO(row), *args, header=0, **kwargs)
        if useWholeFile:
            return PRDD.fromRDD(self.sc.wholeTextFiles(name).map(
                lambda (name, contents): csv_file(contents, *args, **kwargs)))
        else:
            return PRDD.fromRDD(self.sc.textFile(name).mapPartitions(
                lambda x: csv_rows(x, *args, **kwargs)))

    def DataFrame(self, elements, *args, **kwargs):
        """
        Wraps the pandas.DataFrame operation.
        """
        return PRDD.fromRDD(self.sc.parallelize(elements).map(
            lambda element: pandas.DataFrame(data = [element], *args, **kwargs)))

    def stop(self):
        """
        Stop the underlying SparkContext
        """
        self.sc.stop()

if __name__ == "__main__":
    run_tests()
