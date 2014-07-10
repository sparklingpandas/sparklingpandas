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
import unittest
from pandas.util.testing import assert_frame_equal


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
        collectedframe = pframe.collect().sort(['magic'])
        shouldeq = pandas.DataFrame(input, columns=['magic', 'thing']).sort(
            ['magic'])
        self.assertEqual(str(shouldeq), str(collectedframe))

    def test_from_csv_record(self, whole_file=False):
        x = "hi,i,like,coffee\n"
        temp_file = NamedTemporaryFile()
        temp_file.write(x)
        temp_file.flush()
        data = self.psc.read_csv("file://" + temp_file.name,
                                 use_whole_file=whole_file,
                                 names=["1", "2", "3", "4"]).collect()
        expected = pandas.read_csv(
            temp_file.name,
            names=[
                "1",
                "2",
                "3",
                "4"],
            header=None)
        self.assertEqual(str(expected), str(data))

    def test_from_csv_record_whole_file(self):
        self.test_from_csv_record(True)

    def test_from_csv_record_adv(self, whole_file=False):
        x = "person,coffee,tea\nholden,20,2\npanda,1,1"
        temp_file = NamedTemporaryFile()
        temp_file.write(x)
        temp_file.flush()
        data = self.psc.read_csv("file://" + temp_file.name,
                                 use_whole_file=whole_file,
                                 ).collect()
        # Reset the index for comparision.
        data = data.reset_index(drop=True)
        expected = pandas.read_csv(temp_file.name)
        assert_frame_equal(expected, data)

    def test_from_csv_record_adv_whole_file(self):
        self.test_from_csv_record_adv(True)

if __name__ == "__main__":
    unittest.main()
