"""
This script loads a Big Data File Share layer into a Spark DataFrame, parses the geometry info, and writes a new
DataFrame directly to a CSV file in Amazon S3.
"""

from pyspark.sql import *
from pyspark.sql import functions as f
import time



icebergs = spark.read.format("webgis").load(<icebergs url>).select(["SIGHTING_DATE","ICEBERG_NUMBER","$geometry"])

icebergs.show(5)

icebergs_flattened = icebergs.withColumn("longitude", f.col("$geometry.x"))\
                             .withColumn("latitude", f.col("$geometry.y"))\
                             .drop("$geometry")

icebergs_flattened.show(5)

ts_suffix = str(int(time.time()))

icebergs_flattened.write.format("csv").save("s3a://<bucket>/icebergs_" + ts_suffix)