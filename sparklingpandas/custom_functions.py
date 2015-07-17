"""Our magic extend functions. Here lies dragons and a sleepy holden."""
from py4j.java_collections import ListConverter

from pyspark import SparkContext
from pyspark.sql.dataframe import Column, _to_java_column

__all__ = []


def _create_function(name, doc=""):
    """ Create a function for aggregator by name"""
    def _(col):
        spark_ctx = SparkContext._active_spark_context
        java_ctx = getattr(spark_ctx._jvm.com.sparklingpandas.functions,
                     name)(col._java_ctx if isinstance(col, Column) else col)
        return Column(java_ctx)
    _.__name__ = name
    _.__doc__ = doc
    return _

_FUNCTIONS = {
    'kurtosis': 'Calculate the kurtosis, maybe!',
}


def registerSQLExtensions(sqlCtx):
    scala_SQLContext = sqlCtx._ssql_ctx
    spark_ctx = sqlCtx._spark_ctx
    spark_ctx._jvm.com.sparklingpandas.functions.registerUdfs(scala_SQLContext)

for _name, _doc in _FUNCTIONS.items():
    globals()[_name] = _create_function(_name, _doc)
del _name, _doc
__all__ += _FUNCTIONS.keys()
__all__.sort()
