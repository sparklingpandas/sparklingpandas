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
In order to provide an API similar to Pandas' DataFrames or R's Dataframes we
provide the classes [[prdd]]
"""

"""from sparklingpandas.dataframe import DataFrame
from sparklingpandas.pcontext import PSparkContext

__all__ = ["DataFrame", "PSparkContext"]
"""
import os
import sys
from pkg_resources import resource_filename

if 'IS_TEST' not in os.environ and "JARS" not in os.environ:
    VERSION = '0.0.4'
    JAR_FILE = 'sparklingpandas-assembly-' + VERSION + '-SNAPSHOT.jar'
    my_location = os.path.dirname(os.path.realpath(__file__))
    local_prefixes = [
        # For development, use the sbt target scala-2.10 first
        # since the init script is in sparklingpandas move up one dir
        os.path.join(my_location, '../target/scala-2.10/'),
        # Also try the present working directory
        os.path.join(os.getcwd(), '../target/scala-2.10/'),
        os.path.join(os.getcwd(), 'target/scala-2.10/')]
    jars = [os.path.join(prefix, JAR_FILE) for prefix in local_prefixes]
    jars.append(os.path.abspath(resource_filename('sparklingpandas.jar',
                                                  JAR_FILE)))
    try:
        jar = [jar_path for jar_path in jars if os.path.exists(jar_path)][0]
    except IndexError:
        raise IOError("Failed to find jars. Looked at paths %s." % jars)
    os.environ["JARS"] = jar
    os.environ["PYSPARK_SUBMIT_ARGS"] = ("--jars %s --driver-class-path %s" +
                                         " pyspark-shell") % (jar, jar)
