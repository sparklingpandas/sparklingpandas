"""
Provide a way to work with panda data frames in Spark
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
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup
from pyspark.rdd import RDD
from pandaspark.pstatcounter import PStatCounter
import pandas

class PRDD(RDD):
    """
    A Panda Resilient Distributed Dataset (PRDD), is an extension of the RDD.
    It is an RDD containg Panda dataframes and provides special methods that
    are aware of this. All specialized panda functions are prefixed with a p.
    """

    @classmethod
    def fromRDD(cls, rdd):
        """Construct a PRDD from an RDD. No checking or validation occurs"""
        return PRDD(rdd._jrdd, rdd.ctx, rdd._jrdd_deserializer)

    def papplymap(self, f, **kwargs):
        """
        Return a new PRDD by applying a function to each element of each
        Panda DataFrame

        >>> input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        >>> prdd = psc.pDataFrame(input, columns=['magic', 'thing'])
        >>> addpandasfunc = (lambda x: "panda" + x)
        >>> result = prdd.papplymap(addpandasfunc).pcollect()
        >>> str(result.sort(['magic'])).replace(' ','').replace('\\n','')
        'magicthing0pandacoffeepandahappiest0pandateapandahappy0pandawaterpandasad[3rowsx2columns]'
        """
        return self.fromRDD(self.map(lambda data: data.applymap(f), **kwargs))

    def __getitem__(self, key):
        """
        Returns a new PRDD of elements from that key

        >>> input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        >>> prdd = psc.pDataFrame(input, columns=['magic', 'thing'])
        >>> str(prdd['thing'].pcollect()).replace(' ','').replace('\\n','')
        '0happy0sad0happiestName:thing,dtype:object'
        """
        return self.fromRDD(self.map(lambda x: x[key]))

    def pcollect(self):
        """
        Collect the elements in an PRDD and concatenate the partition

        >>> input = [("tea", "happy"), ("water", "sad"), ("coffee", "happiest")]
        >>> prdd = psc.pDataFrame(input, columns=['magic', 'thing'])
        >>> elements = prdd.pcollect()
        >>> str(elements.sort(['magic']))
        '    magic     thing\\n0  coffee  happiest\\n0     tea     happy\\n0   water       sad\\n\\n[3 rows x 2 columns]'
        """
        def appendFrames(frame_a, frame_b):
            return frame_a.append(frame_b)
        return self.reduce(appendFrames)

    def stats(self):
        """
        Compute the stats for each column
        """

if __name__ == "__main__":
    run_tests()
