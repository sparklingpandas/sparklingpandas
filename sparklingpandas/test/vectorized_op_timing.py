import timeit

SETUP_STR = """import pandas as pd
import numpy.random as nprnd
import scipy.stats as scistats
from sparklingpandas.utils import add_pyspark_path
add_pyspark_path()
from pyspark.statcounter import StatCounter
df = pd.DataFrame({'samples' : pd.Series(nprnd.randn(1000))})"""

CMDS = ["df[['samples']].describe()",
        "scistats.describe(df[['samples']].values)",
        "StatCounter(values=df[['samples']].values)"]
N_ITERS = 1000

for command in CMDS:
    command_timer = timeit.Timer(command, SETUP_STR)
    print "Time to execute: '{}'".format(command)
    print command_timer.timeit(N_ITERS) / N_ITERS
