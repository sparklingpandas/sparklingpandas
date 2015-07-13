"""Our magic extend functions. Here lies dragons and a sleepy holden."""
from py4j.java_collections import ListConverter

from pyspark import SparkContext
from pyspark.sql.dataframe import Column, _to_java_column

__all__ = []


def _create_sql_function(name, doc=""):
    """ Create a function for aggregator by name"""
    def _(col):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.com.sparklingpandas.functions, name)(col._jc if isinstance(col, Column) else col)
        return Column(jc)
    _.__name__ = name
    _.__doc__ = doc
    return _

def _create_function_on_df(name, doc=""):
    """ Create a function for calling on Dataframe."""
    def _(df):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.com.sparklingpandas.functions, name)(df._jdf)
        return jc
    _.__name__ = name
    _.__doc__ = doc
    return _


# SQL Functions
_functions = {
    'kurtosis': 'Calculate the kurtosis, maybe!',
}

# Functions on Dataframes
_functions_on_df = {
    'histogram': 'Calculate the histogram',
}

def registerSQLExtensions(sqlCtx):
    scala_SQLContext = sqlCtx._ssql_ctx
    sc = sqlCtx._sc
    sc._jvm.com.sparklingpandas.functions.registerUdfs(scala_SQLContext)

for _name, _doc in _functions.items():
    globals()[_name] = _create_sql_function(_name, _doc)
del _name, _doc
__all__ += _functions.keys()
__all__.sort()
