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
