"""
Test methods in pcontext
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
import json
import os
import tempfile

from sparklingpandas.test.sparklingpandastestcase import \
    SparklingPandasTestCase


class PContextTests(SparklingPandasTestCase):

    def test_dataframe_construction(self):
        input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        dataframe = self.psc.DataFrame(input, columns=['magic', 'thing'])
        elements = dataframe.collect()
        assert len(elements) == 3
        expected = sorted([u'coffee', u'tea', u'water'])
        assert sorted(elements['magic']) == expected

    def test_read_json(self):
        input = [{'magic': 'tea', 'thing': 'happy'},
                 {'magic': 'water', 'thing': 'sad'},
                 {'magic': 'coffee', 'thing': 'happiest'}]
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        with open(temp_file.name, 'wb') as f:
            json.dump(input, f)

        dataframe = self.psc.read_json(temp_file.name)
        elements = dataframe.collect()
        os.unlink(temp_file.name)

        assert len(elements) == 3
        expected = sorted([u'coffee', u'tea', u'water'])
        assert sorted(elements['magic']) == expected
