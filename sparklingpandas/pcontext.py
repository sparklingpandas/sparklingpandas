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

from sparklingpandas.utils import add_pyspark_path, run_tests

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

    def read_csv(self, name, use_whole_file=False, names=None, skiprows=0,
                 *args, **kwargs):
        """
        Read a CSV file in and parse it into panda data frames.
        header: row #s to be used 
        All additional parameters are passed to the read_csv function
        """
        def csv_file(partitionNumber, files):
            file_count = 0
            for filename, contents in files:
                # Only skip lines on the first file
                if partitionNumber == 0 and file_count == 0 and skiprows > 0:
                    yield pandas.read_csv(StringIO(contents), *args, header=None,
                                          skiprows = skiprows, **kwargs)
                else:
                    file_count += 1
                    yield pandas.read_csv(StringIO(contents), *args, header=None,
                                          **kwargs)

        def csv_rows(partitionNumber, rows):
            rc = 0
            for row in rows:
                # Skip the first rows from the first partition if requested
                if partitionNumber != 0 or rc >= skiprows:
                    yield pandas.read_csv(StringIO(row), *args, header=None, **kwargs)
                else:
                    rc += 1

        # Determine the names of the columns in the input
        mynames=None
        if names:
            mynames=names
        else:
            first_line = self.sc.textFile(name).first()
            frame = pandas.read_csv(StringIO(first_line))
            mynames = list(frame.columns.values)
            skiprows += 1

        # Do the actual load
        if use_whole_file:
            return PRDD.fromRDD(self.sc.wholeTextFiles(name).mapPartitionsWithIndex(csv_file))
        else:
            return PRDD.fromRDD(self.sc.textFile(name).mapPartitionsWithIndex(csv_rows))

    def DataFrame(self, elements, *args, **kwargs):
        """
        Wraps the pandas.DataFrame operation.
        """
        return PRDD.fromRDD(self.sc.parallelize(elements).map(
            lambda element: pandas.DataFrame(data=[element], *args, **kwargs)))

    def stop(self):
        """
        Stop the underlying SparkContext
        """
        self.sc.stop()


if __name__ == "__main__":
    run_tests()
