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

"""
This module provides some common test case base for PandaSparkTestCases
"""

from sparklingpandas.utils import add_pyspark_path

add_pyspark_path()
from sparklingpandas.pcontext import PSparkContext
from sparklingpandas.prdd import PRDD
import unittest
import sys


class SparklingPandasTestCase(unittest.TestCase):

    """
    Basic SparklingPandasTestCase, inherit from this class to get a
    PSparkContext as sc.
    """

    def setUp(self):
        """
        Setup the basic panda spark test case. This right now just creates a
        PSparkContext.
        """
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.psc = PSparkContext.simple('local[4]', class_name, batchSize=2)

    def tearDown(self):
        """
        Tear down the basic panda spark test case. This stops the running
        context and does a hack to prevent Akka rebinding on the same port.
        """
        self.psc.stop()
        sys.path = self._old_sys_path
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        self.psc.sc._jvm.System.clearProperty("spark.driver.port")


if __name__ == "__main__":
    unittest.main()
