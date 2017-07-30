'''
@author : rsomvanshi

Nulogy assignment on Entity resolution

'''
from pyspark.sql import SparkSession
from fuzzywuzzy import fuzz
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("EntityResolution").getOrCreate()

# Load DBLP1.csv data into dataframe
dblp_df = spark.read.format("csv").option("header", "true").load("DBLP1.csv")

# Load Scholar.csv data into dataframe
scholar_df = spark.read.format("csv").option("header", "true").load("Scholar.csv")

# Fuzzy search: Using token_set_ratio since it seems to be covering most of the cases. (out-of-order, missing tokens)
def fuzzy_ratio(col1, col2):
    res = fuzz.token_set_ratio(col1, col2)
    return res

# Defined UDF : Gets two columns and calculates fuzzy search score
fuzz_udf = udf(fuzzy_ratio, IntegerType())

# Filtering some data with inner join to avoid cross join on entire data set.
# Assumption is made here: Title and year across two datasets is mostly consistent
# Resolution is done on "author" data
cond = [dblp_df.title == scholar_df.title, dblp_df.year == scholar_df.year]
joined_fuzzy_df = dblp_df.join(scholar_df, cond).withColumn("fuzzy_ratio",fuzz_udf(dblp_df["authors"],
                                                                                   scholar_df["authors"]))

# Get entity resolution with fuzzy search score of 80 or more.
#NOTE: Increasing this number will filter more entities giving more refined output.
results = joined_fuzzy_df.filter(joined_fuzzy_df.fuzzy_ratio > 80).collect()

#results.show(5,truncate=True)

'''
+--------------------+--------------------+--------------------+-------------+----+------+------------+--------------------+--------------------+--------------------+----+------+-----------+
|              idDBLP|               title|             authors|        venue|year|Row_ID|   idScholar|               title|             authors|               venue|year|ROW_ID|fuzzy_ratio|
+--------------------+--------------------+--------------------+-------------+----+------+------------+--------------------+--------------------+--------------------+----+------+-----------+
'''
# Read Row objects and write to CSV in desired format
with open("DBLP_Scholar_perfectMapping_rsomvanshi.csv", "w", encoding="utf-8") as f_out:
    header = "idDBLP,idScholar,DBLP_Match,Scholar_Match,Match_ID"
    f_out.write(header + "\n")
    for result in results:
        f_out.write(result[0] + "," + \
                    result[6] + "," + \
                    result[5] + "," + \
                    # DBLP_Match : Row_ID
                    result[11] + "," + \
                    # Scholar_Match : ROW_ID
                    result[5] +"_" + str(result[11]))
        f_out.write("\n")

# Stop the session
spark.stop()