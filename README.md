# Run Python Script - Samples
**Utilities and sample scripts for the Run Python Script GeoAnalytics Server tool**

For an overview of Run Python Script, check out [this blog post](https://www.esri.com/arcgis-blog/products/geoanalytics-server/analytics/extend-your-big-data-analysis-with-spark/)

_SubmitRPSJob.py_ uses the ArcGIS API for Python to submit a script to the Run Python Script tool on your GeoAnalytics Server. The utility can be used in command line (`python SubmitRPSJob.py -portal https://mydomain.com/portal -username analyst1 -password ilovegis -script C:\workflow1.py`) or in the Python console:
```
from SubmitRPSJob import submit
submit(portal_url="https://mydomain.com/portal",username="analyst1", password="ilovegis", script_path=r"C:\workflow1.py")
```
The _scripts_ folder contains sample python scripts demonstrating functionality included with Run Python Script:
- _read-into-df.py_ - create Spark DataFrames from ArcGIS Enterprise layers 
- _write-to-webgis.py_ - write a Spark DataFrame to an ArcGIS Enterprise layer
- _run-ga-tools.py_ - chain GeoAnalytics tools into an analysis pipeline
- _es-connect.py_ - read data from outside of ArcGIS Enterprise with pyspark
- _s3-connect.py_ - write data outside of ArcGIS Enterprise with pyspark

### Other Resources:
- [REST API documentation](https://developers.arcgis.com/rest/services-reference/run-python-script.htm)
- [Reading and Writing layers in pyspark](https://developers.arcgis.com/rest/services-reference/using-webgis-layers-in-pyspark.htm)
- [Using GeoAnalytics Tasks in Run Python Script](https://developers.arcgis.com/rest/services-reference/using-geoanalytics-tools-in-pyspark.htm)
- [More example scripts](https://developers.arcgis.com/rest/services-reference/run-python-script-examples.htm)
- [ArcGIS API for Python documentation](https://esri.github.io/arcgis-python-api/apidoc/html/arcgis.geoanalytics.manage_data.html#run-python-script)
