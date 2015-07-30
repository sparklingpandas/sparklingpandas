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
SparkingPandas provides support for a Pandas-like api on Spark. Spark
DataFrames do not provide the api that Pandas users are used to in data frames.
In order to provide an API similar to Pandas' Dataframes or R's Dataframes we
provide the classes [[prdd]]
"""

"""from sparklingpandas.dataframe import Dataframe
from sparklingpandas.pcontext import PSparkContext

__all__ = ["Dataframe", "PSparkContext"]
"""
import os
import sys

print "running init"

if 'IS_TEST' not in os.environ:
    VERSION = '0.0.4'
    JAR_FILE = 'sparklingpandas_2.10-' + VERSION + '-SNAPSHOT.jar'
    prefix = os.path.realpath(__file__)
    jar = os.path.join(prefix, '../curret-release/', JAR_FILE)
    os.environ["JARS"] = jar
    os.environ["PYSPARK_SUBMIT_ARGS"] = ("--jars %s --driver-class-path %s" +
                                         " pyspark-shell") % (jar, jar)
else:
    print "Test mode, not setting jar info"
