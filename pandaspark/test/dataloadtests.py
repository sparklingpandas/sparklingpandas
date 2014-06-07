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
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pandas

from tempfile import NamedTemporaryFile
from pandaspark.test.pandasparktestcase import PandaSparkTestCase
import pandas
import sys
import unittest

class DataLoad(PandaSparkTestCase):
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
        shouldeq = pandas.DataFrame(input, columns=['magic', 'thing']).sort(['magic'])
        self.assertEqual(shouldeq.all(), collectedframe.all())

    def test_from_csv_record_per_line(self):
        x = "hi, i, like, coffee\n"
        tempFile = NamedTemporaryFile(delete=True)
        tempFile.write(x)
        tempFile.close()
        data = self.psc.csvfile("file://" + tempFile.name, useWholeFile=False).collect()
        expected = pandas.DataFrame(data = [], columns=[])
        self.assertEqual(data.all(), expected.all())
        

if __name__ == "__main__":
    unittest.main()

