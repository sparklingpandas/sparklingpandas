import timeit

setup_str = """import pandas as pd
import numpy.random as nprnd
import scipy.stats as scistats
from pandaspark.utils import add_pyspark_path
add_pyspark_path()
from pyspark.statcounter import StatCounter
df = pd.DataFrame({'samples' : pd.Series(nprnd.randn(1000))})"""

commands = ["df[['samples']].describe()",
            "scistats.describe(df[['samples']].values)",
            "StatCounter(values=df[['samples']].values)"]
n_iters = 1000

for command in commands:
    command_timer = timeit.Timer(command, setup_str)
    print "Time to execute: '{}'".format(command)
    print command_timer.timeit(n_iters) / n_iters
