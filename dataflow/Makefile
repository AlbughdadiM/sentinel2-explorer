build-template:
	python3.8 -m ${name} \
		--runner DataflowRunner \
		--project ${project} \
		--region us-central1 \
		--staging_location gs://${storage}/staging \
		--temp_location gs://${storage}/runtime \
		--template_location gs://${storage}/templates/${name} \
		--save_main_session True \
		--setup_file $(shell pwd)/setup.py

run:
	gcloud dataflow jobs run sentinel-job-${name}-$(shell date +%s) \
		--region=us-central1 \
		--gcs-location=gs://${storage}/templates/${name} \
		--staging-location=gs://${storage}/staging \
        --service-account-email=gcp-ml-300114@appspot.gserviceaccount.com \
		--parameters=start_date="${start_date}",end_date="${end_date}",tile="${tile}",cloud_cover="${cloud_cover}"
