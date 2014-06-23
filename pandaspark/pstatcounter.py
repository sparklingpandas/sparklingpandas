"""
This module provides statistics for L{PRDD}s.
Look at the stats() method on PRDD for more info.
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
import pandas
add_pyspark_path()

from pyspark.statcounter import StatCounter

class PStatCounter(object):
    """
    A wrapper around StatCounter which collects stats for multiple columns
    """

    def __init__(self, dataframes = [], columns = []):
        """
        Creates a stats counter for the provided data frames
        computing the stats for all of the columns in columns.
        Parameters
        ----------
        dataframes: list of dataframes, containing the values to compute stats on
        columns: list of strs, list of columns to compute the stats on
        """
        self._columns = columns
        self._counters = {column: StatCounter() for column in columns}
 
        for df in dataframes:
            self.merge(df)


    def merge(self, frame):
        """
        Add another DataFrame to the PStatCounter
        >>> import pandas
        >>> from pandaspark.pstatcounter import PStatCounter
        >>> input = [("magic", 10), ("ninja", 20), ("coffee", 30)]
        >>> df = pandas.DataFrame(data = input, columns = ['a', 'b'])
        >>> PStatCounter([df], columns=['b'])
        (field: b,  counters: (count: 3, mean: 20.0, stdev: 8.16496580928, max: 30, min: 10))
        """
        for column, values in frame.iteritems():
            # Temporary hack, fix later
            counter = self._counters.get(column)
            for value in values:
                if counter is not None:
                    counter.merge(value)

    def merge_pstats(self, other):
        """
        Merge all of the stats counters of the other PStatCounter with our counters
        """
        if not isinstance(other, PStatCounter):
            raise Exception("Can only merge PStatcounters!")

        for column, counter in self._counters.items():
            other_counter = other._counters.get(column)
            self._counters[column] = counter.mergeStats(other_counter)

        return self

    def __str__(self):
        str = ""
        for column, counter in self._counters.items():
            str += "(field: %s,  counters: %s)" % (column, counter)
        return str

    def __repr__(self):
        return self.__str__()

if __name__ == "__main__":
    run_tests()
