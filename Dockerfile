# Start with a base image that includes Conda (e.g., the official Miniconda image)
FROM continuumio/miniconda3:latest

# ... (other parts of your Dockerfile)

# Copy the environment.yml file from the parent directory into the image
COPY ../environment.yml /app/environment.yml

# Create the Conda environment from the environment.yml file
RUN conda env create -f /app/environment.yml

# Activate the Conda environment
SHELL ["conda", "run", "-n", "assetx-calibration", "/bin/bash", "-c"]

# Set the entrypoint for the image to start Airflow with the Conda environment activated
ENTRYPOINT ["conda", "run", "-n", "assetx-calibration", "airflow"]

# Set the default command to run Airflow webserver
CMD ["webserver"]