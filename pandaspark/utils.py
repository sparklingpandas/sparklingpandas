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

def run_tests():
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
    msg = "{0} test ran {1} failures".format(test_count, failure_count)
    try:
        # My kingdom for the letter u
        from termcolor import colored
        if failure_count:
            msg = colored(msg, 'red')
        else:
            msg = colored(msg, 'green')
        print msg
    except ImportError:
        if failure_count:
            msg = '\033[91m' + msg 
        else:
            msg = '\033[92m' + msg
        print msg + '\033[0m'
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    run_tests()
