"""
pyspark includes many built-in connectors to data sources other than ArcGIS Enterprise. This provides a way to connect
your GeoAnalytics Server to data sources not supported as Big Data File Shares.
For example, you may want to read data directly from an Elasticsearch cluster and use it with GeoAnalytics, which is
not a workflow possible with Big Data File Shares and stand-alone tools.
This script shows how you can read from Elasticsearch and use the data with the Aggregate Points tool.

Note: Always register a datasource as a Big Data File Share if it is supported. Use the
approach shown in this script only if you must connect to a data source NOT supported by Big Data File Shares.
For more info see https://enterprise.arcgis.com/en/server/latest/get-started/windows/what-is-a-big-data-file-share.htm
"""

from pyspark.sql import *
from pyspark.sql import functions as f

# Print statements will be returned as job messeges from the GeoAnalytics service.
print("LOADING INTO DATAFRAME FROM ELASTICSEARCH...")

# Use read.format("org.elasticsearch.spark.sql") and provide the required options to load a dataset called "hurricanes"
# into a DataFrame.
hurricanes = spark.read.format("org.elasticsearch.spark.sql")\
  .option("es.nodes", <address>)\
  .option("es.port", "9220")\
  .option("es.net.http.auth.user", <user>)\
  .option("es.net.http.auth.pass" , <password>)\
  .load("demo/hurricanes")


print("SCHEMA FROM ES:")
# Print the schema of the DataFrame created from Elasticsearch. In this example the DataFrame has point geometry data
# stored in two columns, one with longitude and one with latitude.
hurricanes.printSchema()

# In order for GeoAnalytics tools to recognize and use the geometry data in this DataFrame, the longitude and latitude
# columns must be combined into a single StructType column, as is shown in the function below.
# This StructType column contains metadata idicating the geometry type: {"geometry": {"type": "point"}}
def add_geometry_from_xy(df, name, x, y):
    projection = f.struct([f.col(x).cast("double").alias("x"), f.col(y).cast("double").alias("y")])
    metadata = {"geometry": {"type": "point"}}
    return df.withColumn(name, projection.alias(name, metadata = metadata)).drop(*[x,y])

# Call add_geometry_from_xy() on the hurricanes DataFrame to create a field called "$geometry" from two fields called
# "longitude" and "latitude".
hurricanes_with_geometry = add_geometry_from_xy(hurricanes, "$geometry", "longitude", "latitude")


print("SCHEMA WITH GEOMETRY")
# Print the scheme of the DataFrame to verify that the "$geometry" column was created correctly.
hurricanes_with_geometry.printSchema()

print("AGGREGATING HURRICANES AND WRITING TO ARCGIS ENTERPRISE...")
# Now that the geometry data is organized into a column that GeoAnalyitcs can understand, the DataFrame can be
# used for spatial analysis. Here the hurricane locations are aggregated into 10 mile hexagon bins.
aggregated = geoanalytics.aggregate_points(hurricanes_with_geometry, bin_type = "Hexagon", bin_size = 10,
                                           bin_size_unit = "miles")
# Write result DataFrame to ArcGIS Enterprise as a feature service layer.
aggregated.write.format("webgis").save("demo_hurricanes_agg")

