from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]").appName("SparkbyExamples.com").getOrCreate()

data = [
    ("George", "M", 12),
    ("Adolf","M", 14),
    ("Emilia","F", 16),
]
print("\nOriginal RDD")
rdd = spark.sparkContext.parallelize(data)
print(rdd)
print("RDD Count ", rdd.count())

print("\nDataFrame from RDD")
df_rdd = rdd.toDF()
df_rdd.show()

print("\nDataFrame from list")
columns = ["Name", "Sex", "Age"]
df_list = spark.createDataFrame(data=data, schema=columns)
df_list.show()
print(df_list.head())

print("\nDataFrame with Schema")
schema = StructType([
    StructField("Name", StringType(), False),
    StructField("Sex", StringType(), True),
    StructField("Age", IntegerType(), True),
])
df_schema = spark.createDataFrame(data=data, schema=schema)
df_schema.printSchema()
df_schema.show(truncate=False)
exit()

print("\nDataFrame from csv")
df_csv = spark.read.csv("test.csv", header=True)
df_csv.printSchema()
df_csv.show()

df_csv.createOrReplaceTempView("PERSON_DATA")
df_sql = spark.sql("SELECT * FROM PERSON_DATA")
df_sql.show()

df_groupby = spark.sql("SELECT Sex, COUNT(*) FROM PERSON_DATA GROUP BY Sex")
df_groupby.show()