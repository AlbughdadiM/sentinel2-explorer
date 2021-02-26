# Download Sentinel-2 Images from Google Cloud Storage (Level L2A and L1C)

[![Linux Build Status](https://github.com/AlbughdadiM/AXA_forest/workflows/dependency-test/badge.svg)](https://github.com/AlbughdadiM/sentinel-google-download/actions) 
[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)
[![Python 3.6](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Python 3.6](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

Python scripts to download full Sentinel-2 products from Google Cloud Storage.

1. **download_s2.py** to search using BigQuery catalog and download the found products to local machine. For this script, a service account is needed with BigQuery and Cloud Storage roles. If you want to use this script, please make sure to change the project_id in the file to your project and to add the name of the BigQuery L2A catalog.

2. **download_products.py** to download a list of products searched via the Dash App. For this script, no service accounts are needed as there is no need to use BigQuery.

In the python script, the logical way to get the full path of each band is using the manifest file. However, I noticed that for a period of time, the manifest file in the L2A product contains the paths of bands in the L1C product, which leads to invalid URLs. So, I decided to use the MTD.xml file to get the paths of the bands.
