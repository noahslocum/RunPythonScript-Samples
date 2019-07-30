"""
This script loads a Big Data File Share layer of tree locations into a Spark DataFrame, calculates a group-by table on
tree species that have poor health, and writes the result to a feature service.
"""
# Packages can be imported if they are installed on each GeoAnalytics Sevrer machine. uuid is included with the
# ArcGIS Server Python environment.
import uuid

# Print statements will be returned as job messeges from the GeoAnalytics service
print("LOADING LAYERS INTO DATAFRAMES...")

# Load a layer of tree locations from a big data file share into a Spark DataFrame
# Data can be found at https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh
layer_url = "https://gax-large.ga.geocloud.com/arcgis/rest/services/DataStoreCatalogs/bigDataFileShares_ga-server-demo-data-bdfs/BigDataCatalogServer/NYC_tree_survey"
NYC_tree_survey = spark.read.format("webgis").load(layer_url)


print("FILTERING AND GROUPING BY SPECIES...")
# Call filter() on the DataFrame to obtain only trees with poor health.
# Then use groupBy() and count() to create a table of the number of trees in poor health per species.
# The result is a DataFrame.
poorhealth_species_count = NYC_tree_survey.filter("health = 'Poor'").groupBy("spc_common").count()


print("WRITING TO ARCGIS ENTERPRISE...")
# Generate a unique output name using uuid package.
# The tool will otherwise fail if the output layer name already exists in ArcGIS Enteprise.
unique_id = str(uuid.uuid4())[:4]
output_name = "NYC_Tree_Count_" + unique_id

# Write DataFrame to ArcGIS Datastore using write.save().
# The "webgis" format string indicates that we are writing to ArcGIS Enteprise.
# When save() is called, a hosted feature service will be created with the provided output name.
# write.save() can also be used to write to a Big Data File Share.
poorhealth_species_count.write.format("webgis").save(output_name)

print("Script complete.")