# import findspark
# findspark.init()
# print(findspark.find())
# exit()
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkbyExamples.com").getOrCreate()

print("\nFrom a list")
rdd = spark.sparkContext.parallelize([1,2,3,4,5,6])
print("RDD Count ", rdd.count())

print("\nFrom a text file")
rdd_text = spark.sparkContext.textFile("test.txt")
print("RDD Count ", rdd_text.count())

# spark.sparkContext.stop()