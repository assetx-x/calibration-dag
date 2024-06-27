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
	docker build -t base_docker:latest -f src/base.Dockerfile .
	docker build -t intermediate_training_image:latest -f src_2/Dockerfile .
	docker build -t gan_image:latest -f src/Dockerfile .
	docker build -t rolling_image:latest -f src_3/Dockerfile .
	docker build -t get_raw_prices_image:latest -f src_4/Dockerfile .
	docker build -t filter_r1k_image:latest -f src_5/Dockerfile .

deploy:
	$(get_credentials)
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade airflow apache-airflow/airflow -f k8s/values.yml --wait --timeout=30m --debug --atomic --namespace default

up:
	@echo "\n[ ] SHUTTING DOWN SERVICES\n"
	docker compose down --volumes --rmi all
	@echo "\n[ ] PULLING CHANGES FROM GIT\n"
	 sudo git pull
	@echo "\n[ ] RUNNING AIRFLOW CONTAINER\n"
	docker compose up --build -d
	@echo "\n[ ] DONE\n"

restart:
	@echo "\n[ ] RESTARTING\n"
	docker compose down
	systemctl restart docker
	docker compose up -d

push:
	export TAG=v0.1.0
	sudo docker build -t calibration-airflow -f Dockerfile .
	sudo docker tag dcm gcr.io/dcm-prod-ba2f/assetx-api:${TAG}
	docker push gcr.io/dcm-prod-ba2f/assetx-api:${TAG}
