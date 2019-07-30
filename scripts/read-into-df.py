"""
This script loads a Big Data File Share layer into a Spark DataFrame, prints the DataFrame schema to the console, and
prints the count of features in the dataset to the console.
"""

# Print statements will be returned as job messeges from the GeoAnalytics service
print("LOADING LAYERS INTO DATAFRAMES...")

# Example data can be found at https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh
# This dataset was registered as a big data file share, which can be referenced using a URL.
layer_url = "https://mydomain.com/arcgis/rest/services/DataStoreCatalogs/bigDataFileShares_demo-bdfs/BigDataCatalogServer/NYC_tree_survey"

# To load the dataset into a Spark DataFrame, use spark.read.
# The "webgis" format string indicates that we are reading from a feature service or Big Data File Share using a URL.
# Any geometry or time info will be added to the DataFrame automatically as "$geometry" and "$time" columns.
NYC_tree_survey = spark.read.format("webgis").load(layer_url)

# DataFrames have built in operations that can be used to interrogate the data.
# Here the printSchema() method is called to print out the dataset schema as a string.
print("SCHEMA:")
NYC_tree_survey.printSchema()

# Here the count() method is called to count the number of features in this dataset.
# This operation will be distributed across your GeoAnalytics Server site.
print("COUNTING FEATURES...")
print("Count of features: {}".format(NYC_tree_survey.count()))