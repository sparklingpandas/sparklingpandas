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

import utils
utils.addPySparkPath()
import pandas
import StringIO
from pyspark.context import SparkContext
from pandaspark.prdd import PRDD

class PSparkContext(SparkContext):
    """
    This is a thin wrapper around SparkContext from
    PySpark which makes it easy to load data into L{PRDD}s.
    """

    def pCSVFile(self, name, useWholeFile = True, **kwargs):
        """
        Read a CSV file in and parse it into panda data frames. Note this uses wholeTextFiles
        underneath the hood so as to support multi-line CSV records so many small input files
        are prefered. Additional parameters are passed to the read_csv function
        """
        def csvFile(contents, **kwargs):
            pandas.read_csv(contents, kwargs)
        def csvRows(rows, **kwargs):
            for row in rows:
                yield pandas.read_csv(row, kwargs)
        if useWholeFile:
            return PRDD._fromRDD(self.wholeTextFiles(path).flatMap(lambda x: csvFile(x, **kwargs)))
        else:
            return PRDD._fromRDD(self.textFiles(path).mapPartitions(lambda x: csvRows(x, **kwargs)))

    def pDataFrame(self, elements, **kwargs):
        """
        Wraps the pandas.DataFrame operation
        >>> prdd = psc.pDataFrame([("tea", "happy"), ("water", "sad"), ("coffee", "happiest")], columns=['magic', 'thing'])
        >>> elements = prdd.collect()
        >>> len(elements)
        3
        >>> sorted(map(lambda x: x['magic'].all(), elements))
        ['coffee', 'tea', 'water']
        """
        return PRDD._fromRDD(self.parallelize(elements).map(lambda element: pandas.DataFrame(data = [element], **kwargs)))

def _test():
    import doctest
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    globs['psc'] = PSparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs,optionflags=doctest.ELLIPSIS)
    globs['psc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
