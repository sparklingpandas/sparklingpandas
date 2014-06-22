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
import nose.tools as nose_tools

def test_pstatcounter():
    from pandaspark.col_stat_counters import ColumnStatCounters
    input = [("magic", 10), ("ninja", 20), ("coffee", 30)]
    df = pandas.DataFrame(data = input, columns = ['a', 'b'])
    counter = ColumnStatCounters([df], columns=['b'])
    b_col_stat_counter = counter._column_stats['b']
    assert b_col_stat_counter.count() == 3
    assert b_col_stat_counter.mean() == 20.0
    nose_tools.assert_almost_equal(b_col_stat_counter.stdev(), 8.16496580928)
    assert b_col_stat_counter.max() == 30
    assert b_col_stat_counter.min() == 10
