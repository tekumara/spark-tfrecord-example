from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession

conf = (SparkConf()
        .setAppName("tfrecord_example")
        .set("spark.ui.enabled", "false"))

spark = SparkSession.builder.config(conf=conf).getOrCreate()

path = "test-output.tfrecord"

fields = [StructField("id", IntegerType()), StructField("IntegerCol", IntegerType()),
          StructField("LongCol", LongType()), StructField("FloatCol", FloatType()),
          StructField("DoubleCol", DoubleType()), StructField("VectorCol", ArrayType(DoubleType(), True)),
          StructField("StringCol", StringType())]
schema = StructType(fields)
test_rows = [[11, 1, 23, 10.0, 14.0, [1.0, 2.0], "r1"], [21, 2, 24, 12.0, 15.0, [2.0, 2.0], "r2"]]
rdd = spark.sparkContext.parallelize(test_rows)
df = spark.createDataFrame(rdd, schema)
df.write.format("tfrecords").option("recordType", "Example").option("codec", "org.apache.hadoop.io.compress.GzipCodec").save(path)

df = spark.read.format("tfrecords").option("recordType", "Example").load(path)
df.show()