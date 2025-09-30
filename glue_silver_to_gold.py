# glue_silver_to_gold.py
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
import sys

args = getResolvedOptions(sys.argv, ['SILVER_S3','GOLD_S3'])
silver = args['SILVER_S3']
gold = args['GOLD_S3']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.parquet(silver)
gold_df = df.groupBy('order_date','product').agg(F.sum('total_price').alias('total_sales'))
gold_df.write.mode('overwrite').partitionBy('order_date').parquet(gold)
print("written gold to", gold)
