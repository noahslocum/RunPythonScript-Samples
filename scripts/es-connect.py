from pyspark.sql import *
from pyspark.sql import functions as f
import time
ts_suffix = str(int(time.time()))

df = spark.read.format("org.elasticsearch.spark.sql")\
  .option("es.nodes", <address>)\
  .option("es.port", "9220")\
  .option("es.net.http.auth.user", <user>)\
  .option("es.net.http.auth.pass" , <password>)\
  .load("demo/hurricanes")\
  .select(["serial_num","longitude","latitude","iso_time"])

print("")
print("Schema from ES")
df.printSchema()


# Set dataframe geometry

def add_geometry_from_xy(df, name, x, y):
    projection = f.struct([f.col(x).cast("double").alias("x"), f.col(y).cast("double").alias("y")])
    metadata = {"geometry": {"type": "point"}}
    return df.withColumn(name, projection.alias(name, metadata = metadata)).drop(*[x,y])

df_with_geometry = add_geometry_from_xy(df, "$geometry", "longitude", "latitude")


print("Schema with geometry")
df_with_geometry.printSchema()


print("Writing raw features")
df_with_geometry.write.format("webgis").save("demo_hurricanes_" + ts_suffix)


print("Writing aggregated features")
aggregated = geoanalytics.aggregate_points(df_with_geometry, bin_type = "Hexagon", 
    bin_size = 10, bin_size_unit = "miles")
    
aggregated.write.format("webgis").save("demo_hurricanes_agg_" + ts_suffix)


