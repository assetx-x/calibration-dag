PHONY: web

web:
	gcloud container clusters get-credentials cluster-1 --zone us-east4-a --project dcm-prod-ba2f
	kubectl port-forward svc/airflow-webserver 8080:8080 --namespace default
	echo "Airflow UI available at http://localhost:8080"

