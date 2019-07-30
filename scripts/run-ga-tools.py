"""
This script loads a Big Data File Share layer of tree locations into a Spark DataFrame, projects the geometry,
clips the dataset to the boundary of Manhattan, calculates the density of dead trees, and clips the density bins 
to the boundary of Manhattan.
"""
# Packages can be imported if they are installed on each GeoAnalytics Sevrer machine. uuid is included with the
# ArcGIS Server Python environment.
import uuid

# Print statements will be returned as job messeges from the GeoAnalytics service.
print("LOADING LAYERS INTO DATAFRAMES...")

# Load a layer of tree locations from a big data file share into a Spark DataFrame.
# Data can be found at https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh
layer_url = "https://mydomain.com/arcgis/rest/services/DataStoreCatalogs/bigDataFileShares_demo-bdfs/BigDataCatalogServer/NYC_tree_survey"
NYC_tree_survey = spark.read.format("webgis").load(layer_url)

# Load a layer of NYC borough boundaries from a big data file share into a DataFrame.
# Data can be found at https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm
boroughs_url = "https://mydomain.com/arcgis/rest/services/DataStoreCatalogs/bigDataFileShares_demo-bdfs/BigDataCatalogServer/NY_Borough_Boundaries"
boroughs = spark.read.format("webgis").load(boroughs_url)

print("PROJECTING DATA...")
# A geoanalytics module is imported automatically when you use Run Python Script and contains all GeoAnalytics tools.
# Each tool accepts DataFrames as input and return DataFrames as output.
# Here a geoanalytics tool is used to project the geometry of each DataFrame to WKID 2263.
NYC_tree_survey_proj = geoanalytics.project(input_features=NYC_tree_survey, output_coord_system=2263)
boroughs_proj = geoanalytics.project(input_features=boroughs, output_coord_system=2263)

# Results of GeoAnalytics tools can be passed to other GeoAnalytics tools to create an analysis pipeline.
# No results will be written out to a data store until write.save() is called
# Here Clip Layer is used to clip the projected layer of tree locations to Manhattan's boundary.
# filter() is called on boroughs_proj to query for Manhattan's boundary.
print("CLIPPING DATA...")
trees_clipped = geoanalytics.clip_layer(NYC_tree_survey_proj, boroughs_proj.filter("boro_name = 'Manhattan'"))

# Pass the result of Clip Layer to Calculate Density to find the kernel density of trees.
# filter() is called on trees_clipped to obtain only trees that are not "Alive".
print("RUNNING CALCULATE DENSITY...")
tree_density = geoanalytics.calculate_density(trees_clipped.filter("status != 'Alive'"), weight="Kernel",
                                              bin_type="Hexagon", bin_size=0.25, bin_size_unit="Miles", radius=0.5,
                                              radius_unit="Miles", area_units="SquareMiles")

# Pass the result of Calculate Density to Clip Layer to clip density bins to Manhattan boundary.
# This results in a density grid with smooth edges rather than the jagged edges.
print("CLIPPING RESULT...")
tree_density_clipped = geoanalytics.clip_layer(tree_density, boroughs_proj.filter("boro_name = 'Manhattan'"))

# Generate a unique output name using uuid package.
# The tool will otherwise fail if the output layer name already exists in ArcGIS Enteprise.
unique_id = str(uuid.uuid4())[:4]
output_name = "manhattan_tree_density_" + unique_id

# Write result to ArcGIS DataStore
# This is the only result that will be written to a data store.
print("WRITING TO ARCGIS ENTERPRISE...")
tree_density_clipped.write.format("webgis").save(output_name)

print("Script complete.")