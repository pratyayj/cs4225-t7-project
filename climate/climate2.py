import random
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

# params
N = 100

# import data
schema = StructType([
  StructField("id", IntegerType(), False),
  StructField("city", StringType(), False),
  StructField("date", StringType(), False),
  StructField("avgMedian", DoubleType(), False)])

# new dataframe to store results
results_schema = StructType([
  StructField("city", StringType(), False),
  StructField("mean", DoubleType(), False), 
  StructField("S_diff", DoubleType(), False), 
  StructField("num_bootstraps", IntegerType(), False), 
  StructField("CI", DoubleType(), False)
])
columns = ["city", "mean", "S_diff", "num_bootstraps", "CI"]

# compute the cumulative sum:
def cumulative_sum(input_data, mean):
  # initialise the values
  S_max = 0
  S_min = float('inf')

  S = 0
  for x in input_data:
    # x = row["avgMedian"]
    diff = x - mean
    S = S + diff
    # print("x =", x, "diff = ", diff, "S = ", S)
    if S > S_max:
      S_max = S
      # print("new S_max = ", S_max)
    if S < S_min:
      S_min = S
      # print("new S_min = ", S_min)

  # get S_diff
  S_diff = S_max - S_min
  # print("S_diff =", S_max, "-", S_min, "=", S_diff)
  return S_diff

def bootstrap(input_data, mean, S_diff, N):
  length = len(input_data)
  num_bootstraps = 0 # number of bootstraps for which S^i_diff < S_diff

  for i in range(1, N):
    # randomise the data
    randomised_data = random.sample(input_data, length)
    # repeat the cumulative sum
    bootstrap_value = cumulative_sum(randomised_data, mean)
    # print(i, bootstrap_value)
    # if bootstrap_value < S_diff, add to num_bootstraps
    if bootstrap_value < S_diff:
      num_bootstraps += 1
      # print("plus one")

  return num_bootstraps

# process pm25
pm25 = spark.read.format("csv").option("header", True).schema(schema).load("pm25_combined_csv.csv")

pm25 = pm25.sort("city", "date")

pm25_results = spark.createDataFrame(spark.sparkContext.emptyRDD(), results_schema)
pm25_cities = pm25.select(f.collect_set("city")).collect()[0][0] # len: 547

for n, city in enumerate(pm25_cities):
  subset = pm25.filter(pm25.city == city).select("date", "avgMedian")
  data = subset.select(f.collect_list("avgMedian")).first()[0]
  mean = subset.agg({"avgMedian": "mean"}).collect()[0][0]
  S_diff = cumulative_sum(data, mean)
  num_bootstraps = bootstrap(data, mean, S_diff, N)
  CI = 100 * num_bootstraps / N
  new_row = spark.createDataFrame([(city, mean, S_diff, num_bootstraps, CI)], columns)
  pm25_results = pm25_results.union(new_row)
  print("Done with city", n, city)

# repeat for temp
temp = spark.read.format("csv").option("header", True).schema(schema).load("temp_combined_csv.csv")
temp = temp.sort("city", "date")

temp_results = spark.createDataFrame(spark.sparkContext.emptyRDD(), results_schema)
temp_cities = temp.select(f.collect_set("city")).collect()[0][0] # len: 616

for n, city in enumerate(temp_cities):
  subset = temp.filter(temp.city == city).select("date", "avgMedian")
  data = subset.select(f.collect_list("avgMedian")).first()[0]
  mean = subset.agg({"avgMedian": "mean"}).collect()[0][0]
  S_diff = cumulative_sum(data, mean)
  num_bootstraps = bootstrap(data, mean, S_diff, N)
  CI = 100 * num_bootstraps / N
  new_row = spark.createDataFrame([(city, mean, S_diff, num_bootstraps, CI)], columns)
  temp_results = temp_results.union(new_row)
  print("Done with city", n, city)

# export data
pm25_results.toPandas().to_csv("final_pm25_results.csv", header=True)
temp_results.toPandas().to_csv("final_temp_results.csv", header=True)