import random

print("LOADING LAYERS INTO DATAFRAMES...")
# Load the a layer of tree locations into a DataFrame
# Data can be found at https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh
layer_url = "https://dev0000587.esri.com/gax/rest/services/DataStoreCatalogs/bigDataFileShares_GAServerDemoData/BigDataCatalogServer/NYC_tree_survey"
NYC_tree_survey = spark.read.format("webgis").load(layer_url)

# Load a layer of NYC borough boundaries into a DataFrame
# Data can be found at https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm
boroughs_url = "https://dev0000587.esri.com/gax/rest/services/DataStoreCatalogs/bigDataFileShares_GAServerDemoData/BigDataCatalogServer/NY_Borough_Boundaries"
boroughs = spark.read.format("webgis").load(boroughs_url)

# Project input data
print("PROJECTING DATA?")
NYC_tree_survey_proj = geoanalytics.project(input_features=NYC_tree_survey, output_coord_system=2263)
boroughs_proj = geoanalytics.project(input_features=boroughs, output_coord_system=2263)

# Clip trees to Manhattan boundary
print("CLIPPING DATA...")
trees_clipped = geoanalytics.clip_layer(NYC_tree_survey_proj, boroughs_proj.filter("boro_name = 'Manhattan'"))

# Calculate kernel density of trees in poor health
print("RUNNING CALCULATE DENSITY?")
tree_density = geoanalytics.calculate_density(trees_clipped.filter("status != 'Alive'"), weight="Kernel",
                                              bin_type="Hexagon", bin_size=0.25, bin_size_unit="Miles", radius=0.5,
                                              radius_unit="Miles", area_units="SquareMiles")

# Clip density bins to Manhattan boundary
print("CLIPPING RESULT...")
tree_density_clipped = geoanalytics.clip_layer(tree_density, boroughs_proj.filter("boro_name = 'Manhattan'"))

# Write result to ArcGIS DataStore
print("WRITING TO ARCGIS ENTERPRISE...")
tree_density_clipped.write.format("webgis").save("manhattan_tree_density_{}".format(random.randrange(10 * 6)))
