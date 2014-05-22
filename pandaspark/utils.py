"""
Simple common utils shared between the pandaspark modules
"""

def add_pyspark_path():
    """
    Add PySpark to the library path based on the value of SPARK_HOME
    """
    import sys
    import os
    try:
        sys.path.append(os.environ['SPARK_HOME']+"/python")
    except KeyError:
        print """SPARK_HOME was not set. please set it. e.g. 
         SPARK_HOME='/home/...' ./bin/pyspark [program]"""
        exit(-1)
