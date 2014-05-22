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
utils.add_pyspark_path()
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
    def _fromRDD(self, rdd):
        """Construct a PRDD from an RDD. No checking or validation is performed"""
        return PRDD(rdd._jrdd, rdd.ctx, rdd._jrdd_deserializer)

    def papplymap(self, f, **kwargs):
        """
        Return a new PRDD by applying a function to each element of each panda dataframe
        
        >>> prdd = psc.pDataFrame([("tea", "happy"), ("water", "sad"), ("coffee", "happiest")], columns=['magic', 'thing'])
        >>> str(prdd.papplymap((lambda x: "panda" + x)).pcollect().sort(['magic'])).replace(' ','').replace('\\n','')
        'magicthing0pandacoffeepandahappiest0pandateapandahappy0pandawaterpandasad[3rowsx2columns]'
        """
        return PRDD._fromRDD(self.map(lambda data: data.applymap(f), **kwargs))

    def __getitem__(self, key):
        """
        Returns a new PRDD of elements from that key
        >>> prdd = psc.pDataFrame([("tea", "happy"), ("water", "sad"), ("coffee", "happiest")], columns=['magic', 'thing'])
        >>> str(prdd['thing'].pcollect()).replace(' ','').replace('\\n','')
        '0happy0sad0happiestName:thing,dtype:object'
        """
        return PRDD._fromRDD(self.map(lambda x: x[key]))

    def pcollect(self):
        """
        Collect the elements in an PRDD and concatenate the partition

        >>> prdd = psc.pDataFrame([("tea", "happy"), ("water", "sad"), ("coffee", "happiest")], columns=['magic', 'thing'])
        >>> elements = prdd.pcollect()
        >>> str(elements.sort(['magic']))
        '    magic     thing\\n0  coffee  happiest\\n0     tea     happy\\n0   water       sad\\n\\n[3 rows x 2 columns]'
        """
        def appendFrames(a, b):
            return a.append(b)
        return self.reduce(appendFrames)

    def stats(self):
        """
        Compute the stats for each column
        """
        

def _test():
    import doctest
    from pcontext import PSparkContext
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
