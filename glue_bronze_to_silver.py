# glue_bronze_to_silver.py
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
import sys

args = getResolvedOptions(sys.argv, ['BRONZE_S3','SILVER_S3'])
bronze = args['BRONZE_S3']
silver = args['SILVER_S3']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.option("header","true").csv(bronze)
df = df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
df = df.withColumn("price", F.col("price").cast(DoubleType()))
df = df.withColumn("total_price", F.col("quantity") * F.col("price"))

# write silver parquet partitioned by order_date
df.write.mode('append').partitionBy('order_date').parquet(silver)
print("written silver to", silver)
