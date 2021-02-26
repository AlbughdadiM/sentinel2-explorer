# Exporting Sentinel-2 L2A catalog to BigQuery

> This is the documentation of the workflow deployment that is responsable for ingesting the L2A catalog to BigQuery.

> Please make sure to change the parameter "YOUR_PROJECT" in these instruction and the Yaml file to the project of deployment.

## How to deploy the WorkFlow to ingest the catalog CSV data available of GCP public bucket to your own BigQuery dataset 

### Create a WorkFlow service account with roles as bigquery admin and storage object admin

```bash
gcloud iam service-accounts create workflow-sa --display-name workflow-sa
```

```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT --member serviceAccount:workflow-sa@YOUR_PROJECT.iam.gserviceaccount.com --role roles/roles/bigquery.admin --role roles/storage.objectAdmin
```

### Deploy the WorkFlow to GCP 

```bash
gcloud workflows deploy load-workflow --location=europe-west4 --description='S2L2A workflow' --source=./workflow.yaml \
--project YOUR_PROJECT --service-account=workflow-sa@YOUR_PROJECT.iam.gserviceaccount.com
```

### Create a service account that will be used with Cloud Scheduler to run the workflow with workflow invoker role

```bash
gcloud iam service-accounts create trigger-sa --display-name trigger-sa
```

```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT --member serviceAccount:trigger-sa@YOUR_PROJECT.iam.gserviceaccount.com \
--role roles/workflows.invoker
```
### Create a Cloud Scheduler job with the following params

param:

    target: HTTP
    url: https://workflowexecutions.googleapis.com/v1/projects/YOUR_PROJECT/locations/europe-west4/workflows/load-workflow/executions
    Auth header: Add OAuth token
    trigger-sa@YOUR_PROJECT.iam.gserviceaccount.com
