"""
This script loads a Big Data File Share layer into a Spark DataFrame, prints the DataFrame schema to the console, and
prints the count of features in the dataset to the console.
"""
import time

print("LOADING LAYERS INTO DATAFRAMES...")
# Load the a layer of tree locations into a DataFrame
# Data can be found at https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh
layer_url = "https://mydomain.com/arcgis/rest/services/DataStoreCatalogs/bigDataFileShares_demo-bdfs/BigDataCatalogServer/NYC_tree_survey"
NYC_tree_survey = spark.read.format("webgis").load(layer_url)

# Show DataFrame schema
print("SCHEMA:")
NYC_tree_survey.printSchema()

# Count number of features
print("COUNTING FEATURES...")
print("Count of features: {}".format(NYC_tree_survey.count()))

time.sleep(5)