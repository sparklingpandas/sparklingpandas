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
import StringIO
from pyspark.context import SparkContext
from pandaspark.prdd import PRDD

class PSparkContext(SparkContext):
    """
    This is a thin wrapper around SparkContext from
    PySpark which makes it easy to load data into L{PRDD}s.
    """

    def pcsvfile(self, name, useWholeFile=True, **kwargs):
        """
        Read a CSV file in and parse it into panda data frames. Note this uses
        wholeTextFiles by default underneath the hood so as to support
        multi-line CSV records so many small input files are prefered.
        All additional parameters are passed to the read_csv function
        """
        # TODO(holden): string IO stuff
        def csv_file(contents, **kwargs):
            pandas.read_csv(contents, kwargs)
        def csv_rows(rows, **kwargs):
            for row in rows:
                yield pandas.read_csv(row, kwargs)
        if useWholeFile:
            return PRDD.fromRDD(self.wholeTextFiles(name).flatMap(
                lambda x: csv_file(x, **kwargs)))
        else:
            return PRDD.fromRDD(self.textFile(name).mapPartitions(
                lambda x: csv_rows(x, **kwargs)))

    def pDataFrame(self, elements, **kwargs):
        """
        Wraps the pandas.DataFrame operation
        >>> input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        >>> prdd = psc.pDataFrame(input, columns=['magic', 'thing'])
        >>> elements = prdd.collect()
        >>> len(elements)
        3
        >>> sorted(map(lambda x: x['magic'].all(), elements))
        ['coffee', 'tea', 'water']
        """
        return PRDD.fromRDD(self.parallelize(elements).map(
            lambda element: pandas.DataFrame(data = [element], **kwargs)))

if __name__ == "__main__":
    run_tests()
