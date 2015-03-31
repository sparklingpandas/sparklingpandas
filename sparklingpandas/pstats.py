"""Provide a way to work with panda data frames in Spark"""
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

class PStats:
    """A object to wrap the stats/aggregation values"""
    def __init__(self, prdd):
        self._df = prdd.collect()
    def __getitem__(self, key):
        return PStatsOnColumn(self._df, key)
class PStatsOnColumn:
    def __init__(self, df, key):
        self._df = df
        self._key = key

    def min():
        return self._df["MIN("+_key+")"]

    def max():
        return self._df["MAX("+_key+")"]

    def avg():
        return self._df["AVG("+_key+")"]

    def sum():
        return self._df["SUM("+_key+")"]

    def count():
        return self.sum()
