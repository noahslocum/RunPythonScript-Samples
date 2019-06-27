# Data can be found at https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh
layer_url = "https://dev0005368.esri.com/gax/rest/services/DataStoreCatalogs/bigDataFileShares_GAServerDemoData/BigDataCatalogServer/NYC_tree_survey"

# Load the big data file share layer into a DataFrame
NYC_tree_survey = spark.read.format("webgis").load(layer_url)

# Show DataFrame schema
print("SCHEMA:")
NYC_tree_survey.printSchema()

# Count number of features
print("COUNTING FEATURES..")
tree_count = NYC_tree_survey.count()
print("Count of features: " + count)