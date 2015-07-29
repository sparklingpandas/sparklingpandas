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

if 'IS_TEST' not in os.environ and "JARS" not in os.environ:
    VERSION = '0.0.4'
    JAR_FILE = 'sparklingpandas-assembly-' + VERSION + '-SNAPSHOT.jar'
    my_location = os.path.dirname(os.path.realpath(__file__))
    prefixes = [
        # For development, use the sbt target scala-2.10 first
        # since the init script is in sparklingpandas move up one dir
        os.path.join(my_location, '../target/scala-2.10/'),
        # Also try the present working directory
        os.path.join(os.getcwd(), '../target/scala-2.10/'),
        os.path.join(os.getcwd(), 'target/scala-2.10/'),
        # For "production" look at some places where we might be installed
        # sys.prefix gives us /usr on many systems and our jars end up in
        # /usr/local
        os.path.join(my_location, '../current-release/'),
        os.path.join(sys.prefix, "local/jars/"),
        # For virtual env our jars end up directly under sys.prefix
        os.path.join(sys.prefix, "jars/"),
        # Since we are installed in a libs/python-version/sparkling panadas
        # directory, jump three up and go to the jars file.
        os.path.join(my_location, '../../../jars/')]
    jars = map(lambda prefix: os.path.join(prefix, JAR_FILE), prefixes)
    try:
        jar = filter(lambda path: os.path.exists(path), jars)[0]
    except IndexError:
        raise IOError("Failed to find jars " + str(jar))
    os.environ["JARS"] = jar
    os.environ["PYSPARK_SUBMIT_ARGS"] = ("--jars %s --driver-class-path %s" +
                                         " pyspark-shell") % (jar, jar)
