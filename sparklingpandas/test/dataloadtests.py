"""
Test all of the methods for loading data into PandaSpark
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

import pandas

from tempfile import NamedTemporaryFile
from sparklingpandas.test.sparklingpandastestcase import \
    SparklingPandasTestCase
import pandas
import sys
import unittest2
from pandas.util.testing import assert_frame_equal
from pandas.core.api import DataFrame
import numpy as np


class DataLoad(SparklingPandasTestCase):

    """
    Class of data loading tests.
    """

    def test_from_tuples(self):
        """
        Test loading the data from a python tuples.
        """
        input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        pframe = self.psc.DataFrame(input, columns=['magic', 'thing'])
        data = pframe.collect().sort(['magic'])
        expected = pandas.DataFrame(input, columns=['magic', 'thing']).sort(
            ['magic'])
        assert_frame_equal(data, expected)

    def test_from_csv_record_whole_file(self):
        x = "hi,i,like,coffee\n"
        temp_file = NamedTemporaryFile()
        temp_file.write(x)
        temp_file.flush()
        data = self.psc.csvfile("file://" + temp_file.name,
                                use_whole_file=True,
                                names=["1", "2", "3", "4"]).collect()
        expected = pandas.read_csv(
            temp_file.name,
            names=[
                "1",
                "2",
                "3",
                "4"],
            header=0)
        assert_frame_equal(data, expected)

    def test_load_from_data_frame(self):
        df = DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
                              'foo', 'bar', 'foo', 'foo'],
                        'B': ['one', 'one', 'two', 'three',
                              'two', 'two', 'one', 'three'],
                        'C': np.random.randn(8),
                        'D': np.random.randn(8)})
        ddf = self.psc.from_data_frame(df)
        ddfc = ddf.collect()
        assert_frame_equal(ddfc, df)

if __name__ == "__main__":
    unittest2.main()
