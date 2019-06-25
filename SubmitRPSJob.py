"""
This utility reads the contents of a Python script and submits the code to the Run Python Script tool in
ArcGIS GeoAnalytics Server.

The Run Python Script tool executes Python code on your GeoAnalytics Server site and enables chaining of
GeoAnalytics tools and pyspark functionality
To learn more about Run Python Script see https://developers.arcgis.com/rest/services-reference/run-python-script.htm

Example usage:
RPS-runner.py -portal https://mydomain.com/portal -username analyst1 -password ilovegis -script C:\workflow1.py
"""

import arcgis.env
import argparse
import sys

if __name__ == "__main__":

    # Parse script arguments
    parser = argparse.ArgumentParser(
        description='Executes a Python script using the Run Python Script tool in ArcGIS GeoAnalytics Server')
    parser.add_argument('-portal', dest='portal', action='store', help='Portal URL')
    parser.add_argument('-username', dest='username', action='store', help='Portal username')
    parser.add_argument('-password', dest='password', action='store', help='Portal password')
    parser.add_argument('-script', dest='script', action='store', help='Full path to Python script')
    args = parser.parse_args()

    # Check that a Portal URL, username, password, and path to python script have been provided
    portal_url = vars(args)["portal"]
    username = vars(args)["username"]
    password = vars(args)["password"]
    script_path = vars(args)["script"]
    if None in [portal_url, username, password, script_path]:
        print("ERROR: Please specify a portal URL, username, password, and path to script.\n")
        parser.print_help()
        sys.exit(1)

    # Read script content using file path provided by user
    try:
        with open(script_path, "r") as r:
            code = r.read()
    except Exception as e:
        print("ERROR: Could not read script, check that file path is valid and try again.\n" +
              "Details: " + str(e))
        sys.exit(1)

    try:
        print("Connecting to ArcGIS Enterprise...")
        gis = arcgis.gis.GIS(url=portal_url, username=username, password=password)
    except Exception as e:
        print("ERROR: Could not connect to ArcGIS Enterprise.\n" +
              "Details: " + str(e))
        sys.exit(1)

    # Check if GeoAnalytics Server is enabled in Portal
    if not arcgis.geoanalytics.is_supported(gis=gis):
        print("ERROR: GeoAnalytics is not enabled in your Portal, cannot use the Run Python Script tool.")
        sys.exit(1)

    # Enable detailed messaging for tracking tool progress
    arcgis.env.verbose = True

    # Execute the Run Python Script tool
    print("Submiting job to GeoAnalytics Server...")
    arcgis.geoanalytics.manage_data.run_python_script(gis=gis, code=code)

