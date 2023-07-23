# Start with a base image that includes Conda (e.g., the official Miniconda image)
FROM apache/airflow:2.6.3
#RUN sh -c 'curl -L "https://repo.anaconda.com/miniconda/Miniconda3-py39_23.5.2-0-Linux-x86_64.sh"'

COPY --from=continuumio/miniconda3:4.12.0 /opt/conda /opt/conda


###################################################################################### TESTING HERE
#FROM solr:8
###USER root
###WORKDIR /app
###COPY environment.yml .

###RUN mkdir -p /conda_env
###RUN chown solr:solr /conda_env
###USER solr
###RUN conda env create -f environment.yml

#WORKDIR /app
#COPY environment.yml .
#RUN mkdir /conda_env
#RUN conda env create -f environment.yml


#################################################################################
ENV PATH=/opt/conda/bin:$PATH
# ... (other parts of your Dockerfile)

# Create a new directory to store the Conda environment
USER root
RUN mkdir -p /conda_env

# Copy the environment.yml file from the parent directory into the image
COPY ../environment_truncated.yml /app/environment_truncated.yml

# Create the Conda environment from the environment.yml file
RUN pip install -U pip
RUN conda env create -f /app/environment_truncated.yml

# Activate the Conda environment
SHELL ["conda", "run", "-n", "assetx-calibration", "/bin/bash", "-c"]

# Set the entrypoint for the image to start Airflow with the Conda environment activated
ENTRYPOINT ["conda", "run", "-n", "assetx-calibration", "airflow"]

# Set the default command to run Airflow webserver
CMD ["webserver"]