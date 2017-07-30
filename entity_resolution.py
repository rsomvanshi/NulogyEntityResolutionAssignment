from pyspark.sql import SparkSession
from fuzzywuzzy import fuzz
from pyspark.sql.types import *
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("EntityResolution").getOrCreate()

# Load DBLP1.csv data into dataframe
dblp_df = spark.read.format("csv").option("header", "true").load("DBLP1.csv")

# Load Scholar.csv data into dataframe
scholar_df = spark.read.format("csv").option("header", "true").load("Scholar.csv")


def fuzzy_ratio(col1, col2):
    res = fuzz.token_set_ratio(col1, col2)
    return res

fuzz_udf = udf(fuzzy_ratio,IntegerType())

cond = [dblp_df.title == scholar_df.title, dblp_df.year == scholar_df.year]
joined_fuzzy_df = dblp_df.join(scholar_df, cond).withColumn('fuzzy_ratio',fuzz_udf(dblp_df['authors'],
                                                                                   scholar_df['authors']))

results = joined_fuzzy_df.filter(joined_fuzzy_df.fuzzy_ratio > 80).collect()

results.show(5,truncate=True)

# Stop the session
spark.stop()