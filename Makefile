PHONY: web

web:
	gcloud container clusters get-credentials cluster-1 --zone us-east4-a --project dcm-prod-ba2f
	kubectl port-forward svc/airflow-webserver 8080:8080 --namespace default | grep -v Handling
	echo "Airflow UI available at http://localhost:8080"

build:
	docker build -t gcr.io/dcm-prod-ba2f/airflow:latest .

deploy:
	gcloud container clusters get-credentials cluster-1 --zone us-east4-a --project dcm-prod-ba2f
	helm install airflow apache-airflow/airflow -f k8s/values.yml --wait --timeout=30m --debug --atomic --namespace default