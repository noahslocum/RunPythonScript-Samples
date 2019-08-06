"""
This utility reads the contents of a Python script and submits the code to the Run Python Script tool in
ArcGIS GeoAnalytics Server.

The Run Python Script tool executes Python code on your GeoAnalytics Server site and enables chaining of
GeoAnalytics tools and pyspark functionality
To learn more about Run Python Script see https://developers.arcgis.com/rest/services-reference/run-python-script.htm

Example CLI usage:
SubmitRPSJob.py -portal https://mydomain.com/portal -username analyst1 -password ilovegis -script C:\workflow1.py

Example Python usage:
from SubmitRPSJob import submit
submit(profile="demo1", script_path="C:\workflow1.py")
"""

import arcgis.env
import argparse
import sys


def submit(script_path, portal_url=None, username=None, password=None, profile=None):

    # Check that a Portal URL, username, password, and path to python script have been provided
    # A GIS profile can also be used to access ArcGIS Enterprise.
    # See https://developers.arcgis.com/python/guide/
    # working-with-different-authentication-schemes/#Storing-your-credentialls-locally
    if script_path is None:
        print("ERROR: Please specify a path to the script to run.\n")
        parser.print_help()
        sys.exit(1)
    if not (portal_url and username and password):
        if not profile:
            print("ERROR: Please specify a Portal URL, username, and password OR a GIS profile\n")
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
        if portal_url and username and password:
            gis = arcgis.gis.GIS(url=portal_url, username=username, password=password, profile=profile)
        else:
            gis = arcgis.gis.GIS(profile=profile)
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
    # https://developers.arcgis.com/rest/services-reference/run-python-script.htm
    # https://esri.github.io/arcgis-python-api/apidoc/html/arcgis.geoanalytics.manage_data.html#run-python-script
    print("Submitting job to GeoAnalytics Server...")
    arcgis.geoanalytics.manage_data.run_python_script(gis=gis, code=code)


if __name__ == "__main__":
    # Parse script arguments
    parser = argparse.ArgumentParser(
        description='Executes a Python script using the Run Python Script tool in ArcGIS GeoAnalytics Server')
    parser.add_argument('-portal', dest='portal', action='store', help='Portal URL')
    parser.add_argument('-username', dest='username', action='store', help='Portal username')
    parser.add_argument('-password', dest='password', action='store', help='Portal password')
    parser.add_argument('-profile', dest='profile', action='store', help='GIS profile (stored credentials)')
    parser.add_argument('-script', dest='script', action='store', help='Full path to Python script')
    args = parser.parse_args()
    url = vars(args)["portal"]
    u = vars(args)["username"]
    pw = vars(args)["password"]
    prf = vars(args)["profile"]
    s = vars(args)["script"]

    # Submit job
    submit(s, url, u, pw, prf)
