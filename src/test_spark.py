from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SparkEjercicio")
    .getOrCreate()
)

print(spark.version)

df = spark.read.csv("data/input/sales.csv", header=True)
df.show()

spark.stop()