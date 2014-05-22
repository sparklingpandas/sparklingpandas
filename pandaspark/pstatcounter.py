"""
This module provides statistics for L{PRDD}s. Look at the stats() method on PRDD for more info.
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

from pyspark.statcounter import StatCounter
import pandas

class PStatCounter(object):
    """
    A wrapper around StatCounter which collects stats for multiple columns
    """
    def __init__(self, values=[]):
        self._counters = dict()
        for v in values:
            self.merge(v)

    def merge(self, frame):
        for column, values in frame.iteritems():
            counter = None
            try:
                counter = self._counters.get(key)
                for value in values:
                    counter.merge(v)
            except KeyError:
                c = new StatCounter(values)

    def mergePStats(self, other):
        if not isinstance(other, PStatCounter):
            raise Exception("Can only merge PStatcounters!")

        for column, counter in other._conters.items():
            try:
                self._counters.get(column).mergeStats(counter)
            except KeyError:
                self._counters[column] = counter

