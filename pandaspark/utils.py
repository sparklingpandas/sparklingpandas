def addPySparkPath():
    import sys
    import os
    try:
        sys.path.append(os.environ['SPARK_HOME']+"/python")
    except KeyError:
        print "SPARK_HOME was not set. please set it. e.g. SPARK_HOME='/home/...' ./bin/pyspark [program]"
        exit(-1)
