# GCP Dataflow to calculate NDVI from Sentinel-2 products available on Cloud Storage

This is a dataflow implementation to estimate NDVI for a temporal series of images using GCP Dataflow. If you are tired from downloading images locally to do this kind of porcessing, this can be a good start. The code works on Sentinel-2 L1C products but can be easily extended to L2A if you arelady have the catalog on BigQuery and by copy paste some functions from the other repo of Sentinel-2 explorer. Hopefully, this can be a first point to start developing more concrete image processing applications with Google Services on the cloud.


## How to deploy on GCP

Make sure to export GOOGLE_APPLICATION_CREDENTIALS from your local machine or in the Cloud Console

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa_file.json
```

This means that you already have the key for a service account that have enough roles to perform BigQuery tasks and run Dataflow.

Run the following command to build the pipeline using dataflow-runners

```bash
make build-template name=ndvi_dataflow project=$(gcloud config get-value project) storage=gcp-ml-88 
```

Run the following command to start a job

```bash
make run name=pubsub_dataflow storage=gcp-ml-88 start_date=2017-07-05 end_date=2017-07-20 tile=31TCJ cloud_cover=20
```
