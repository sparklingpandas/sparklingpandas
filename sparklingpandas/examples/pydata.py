profiles = sc.parallelize(list(range(8,12))) # later 1 -> 300

import re
pronoun_re = re.compile("[\s\.\>](she|he|they|xe|ze|zhe|mer)[\s\.\<]", flags=re.IGNORECASE)
 
def fetchProfile(id):
    """Fetch a PyData Seattle speaker profile and return the most common pronoun used"""
    import urllib2
    from collections import Counter
    try:
        response = urllib2.urlopen("http://seattle.pydata.org/speaker/profile/%s" % id)
        html = response.read()
        # Extract the pronouns
        pronouns = re.findall(pronoun_re, html)
        pronouns = map(lambda s: s.lower(), pronouns)
        # Choose the most common
        pronouns_counter = Counter(pronouns).most_common
        if pronouns_counter is not None:
            return [(int(id), pronouns_counter(1)[0][0])]
    except urllib2.HTTPError:
        return []
    except IndexError:
        return [(id, "none")]

from pyspark.sql import SQLContext
from pyspark.sql.types import *
schema = StructType([StructField("id", IntegerType(), True),
         StructField("pronoun", StringType(), True)])
speaker_pronouns = psc.from_spark_df(
    sqlContext.createDataFrame(
        profiles.flatMap(fetchProfile),
        schema
    ))
speaker_pronouns._index_names = ["id"]
speaker_pronouns["pronoun"].histogram()
