get_credentials := gcloud container clusters get-credentials cluster-1 --zone us-east4-a --project dcm-prod-ba2f
docker_build := docker build
image := gcr.io/dcm-prod-ba2f/airflow:latest
image_gan := gan_image:latest
intermediate_training_image := intermediate_training_image:latest
docker_file_src := src/Dockerfile
docker_compose := docker compose

.PHONY: web build deploy rebuild

web:
	$(get_credentials)
	kubectl port-forward svc/airflow-webserver 8080:8080 --namespace default | grep -v Handling
	echo "Airflow UI available at http://localhost:8080"

build:
	docker build -t docker_base:latest -f src/Dockerfile.base .
	docker build -t $(intermediate_training_image) --build-arg $(PWD)=src .
    docker build -t $(image_gan) --build-arg $(PWD)=src_2 .


deploy:
	$(get_credentials)
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade airflow apache-airflow/airflow -f k8s/values.yml --wait --timeout=30m --debug --atomic --namespace default

rebuild:
	@echo "\n[ ] SHUTTING DOWN SERVICES\n"
	$(docker_compose) down
	@echo "\n[ ] PULLING CHANGES FROM GIT\n"
	sudo git pull
	@echo "\n[ ] RUNNING AIRFLOW CONTAINER\n"
	$(docker_compose) up -d
	@echo "\n[ ] DONE\n"
