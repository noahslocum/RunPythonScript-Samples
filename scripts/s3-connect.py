"""
Only DataFrames written with format("webgis") will be available in ArcGIS Enteprise. However, there may be times
where you would want to instead use a different write format (there are many included with pyspark) to save a DataFrame.
For example, you may want to write out data as JSON, which is not supported in Big Data File Shares.
This script shows how you can write json files directly to an Amazon S3 bucket without ArcGIS Enterprise
using format("json").

Note: To write a dataset to an Amazon S3 bucket as a Big Data File Share (which would be available in ArcGIS
Enterprise as a datasource for other GeoAnalytics tools) see
https://developers.arcgis.com/rest/services-reference/using-webgis-layers-in-pyspark.htm#ESRI_SECTION1_6911CFB380294FD790F13C7B5974DF8C
"""
from pyspark.sql import functions as f


# Print statements will be returned as job messeges from the GeoAnalytics service.
print("LOADING LAYERS INTO DATAFRAMES...")

# Load a layer of iceberg sightings from a big data file share into a Spark DataFrame.
# .select() is used to choose only 3 columns from the dataset that will be loaded into the DataFrame.
icebergs = spark.read.format("webgis").load(<icebergs url>).select(["SIGHTING_DATE","ICEBERG_NUMBER","$geometry"])

# Print the first five rows in the DataFrame.
icebergs.show(5)

# Geometry data is stored as a StructType in a column called "$geometry". For point features, this means that "$geometry"
# is single column containing both x and y coordinates. GeoAnalytics Server tools understand this StructType natively,
# but in order to write it external to ArcGIS Enterpise the data needs to be extracted into numeric or string
# columns first.

# To represent the geometry of each feature in a way that can be written to file, withColumn() is used to pull out
# the x and y coordinates into two new columns called "longitude" and "latitude".
# drop() is then used to remove the "$geometry" column.
icebergs_flattened = icebergs.withColumn("longitude", f.col("$geometry.x"))\
                             .withColumn("latitude", f.col("$geometry.y"))\
                             .drop("$geometry")

# Write the DataFrame directly to a S3 bucket using format("json").
# In this case the GeoAnalytics Server user must have write access to the S3 bucket, through an IAM role for example.
# The result will be stored in multiple .json files in a folder called "icebergs".
icebergs_flattened.write.format("json").save("s3a://<bucket>/icebergs")