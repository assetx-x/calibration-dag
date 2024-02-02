get_credentials := gcloud container clusters get-credentials cluster-1 --zone us-east4-a --project dcm-prod-ba2f
docker_build := docker build
image := gcr.io/dcm-prod-ba2f/airflow:latest
image_gan := gan_image:latest
docker_file_src := src/Dockerfile
docker_compose := docker compose

.PHONY: web build deploy rebuild

web:
	$(get_credentials)
	kubectl port-forward svc/airflow-webserver 8080:8080 --namespace default | grep -v Handling
	echo "Airflow UI available at http://localhost:8080"

build:
	$(docker_build) -t $(image) .
	$(docker_build) -f $(docker_file_src) -t $(image_gan) .

deploy:
	$(get_credentials)
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade airflow apache-airflow/airflow -f k8s/values.yml --wait --timeout=30m --debug --atomic --namespace default

rebuild:
	$(docker_compose) down
	sudo git pull
# 	echo -n y | docker system prune -a --volumes
	$(docker_build) -f $(docker_file_src) -t $(image_gan) .
	$(docker_compose) up --build -d
