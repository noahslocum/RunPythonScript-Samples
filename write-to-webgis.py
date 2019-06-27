# Data can be found at https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh
layer_url = "https://dev0005368.esri.com/gax/rest/services/DataStoreCatalogs/bigDataFileShares_GAServerDemoData/BigDataCatalogServer/NYC_tree_survey"

# Load the big data file share layer into a DataFrame
print("LOADING LAYER INTO DATAFRAME...")
NYC_tree_survey = spark.read.format("webgis").load(layer_url)

# Group by species and calculate average of numeric fields
print("GROUPING BY SPECIES...")
groupby_species = NYC_tree_survey.groupBy("spc_common").count()

# Write to ArcGIS Datastore (spatiotemporal by default)
print("WRITING TO ARCGIS ENTERPRISE...")
groupby_species.write.format("webgis").save("NYC_Tree_Species_Count")




