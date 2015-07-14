"""
Simple common utils shared between the sparklingpandas modules
"""
import sys
import os

from glob import glob

def setup_pyspark():
    """Setup pyspark related properties"""

    add_pyspark_path()
    import os
    jars = os.path.realpath(__file__ + "/../../target/scala-2.10/sparklingpandas-assembly-0.0.2-SNAPSHOT.jar")
    os.environ['PYSPARK_SUBMIT_ARGS']="--jars %s --driver-class-path %s pyspark-shell".format(jars, jars)
    add_pyspark_path()

def add_pyspark_path():
    """Add PySpark to the library path based on the value of SPARK_HOME. """

    try:
        spark_home = os.environ['SPARK_HOME']

        sys.path.append(os.path.join(spark_home, 'python'))
        py4j_src_zip = glob(os.path.join(spark_home, 'python',
                                         'lib', 'py4j-*-src.zip'))
        if len(py4j_src_zip) == 0:
            raise ValueError('py4j source archive not found in %s'
                             % os.path.join(spark_home, 'python', 'lib'))
        else:
            py4j_src_zip = sorted(py4j_src_zip)[::-1]
            sys.path.append(py4j_src_zip[0])
    except KeyError:
        print("""SPARK_HOME was not set. please set it. e.g.
        SPARK_HOME='/home/...' ./bin/pyspark [program]""")
        exit(-1)
    except ValueError as e:
        print(str(e))
        exit(-1)
