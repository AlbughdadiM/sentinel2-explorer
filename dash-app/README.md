# Dash App for Sentinel-2 Explorer

## The structure of this app is as follows

1. **main.py** where the layout and callbacks of the dash app are implemented.

2. **app.yaml** to deploy the app to GCP App Engine.

3. **shared** a folder that contains the following

* A service account key: ensure that the service account used to run the web app has the required roles to use BigQuery and App Engine (on my implementation, I took the easy way and granted the service account an Owner permission on the project. However, this is not a good practice).

* **tools.py** contains python functions to perform quries on BigQuery tables (L2A, L1C) and the function required to get the download links from the metadata of Sentinel-2 products.

4. **data** a folder that contains a shapefile with all Sentinel-2 tiles.

5. **assets** a folder that contains css and js functions.

6. **requirements.txt** app requirements.

## Before deploying the app or use it locally, please make sure to change the following

1. Set the mapbox token in main.py

2. Copy the key of the service file to sa_file.json in shared folder

3. Set the project_id in tools.py to your project and set the name of your L2A catalog in BigQuery

## Local deployment

- If you want to deploy the app locally for development or testing, make sure to install google python SDK and to configure your gcloud environment locally. 

- Make sure that the requirements from the requirements.txt are installed.

- Make sure the ```app.run_server(port=8080)``` is not commented in the main.py.

## GCP deployment

- Comment ```app.run_server(port=8080)``` in the main.py.

- In your GCP project from Cloud Console, or locally if gcloud is configured locally run ```gcloud app deploy```