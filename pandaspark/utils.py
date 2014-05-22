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
        sys.path.append(os.environ['SPARK_HOME'] + "/python")
        sys.path.append(os.environ['SPARK_HOME'] +
                        "/python/lib/py4j-0.8.1-src.zip")
    except KeyError:
        print """SPARK_HOME was not set. please set it. e.g.
         SPARK_HOME='/home/...' ./bin/pyspark [program]"""
        exit(-1)

def _test():
    """
    Setup and run the doc tests.
    """
    import doctest
    from pandaspark.pcontext import PSparkContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    globs['psc'] = PSparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs,
                                                  optionflags=doctest.ELLIPSIS)
    globs['psc'].stop()
    if failure_count:
        exit(-1)
