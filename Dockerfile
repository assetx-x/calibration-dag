# Start from the Apache Airflow image
FROM apache/airflow:2.6.3-python3.8

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt


#############################################
# Set the working directory to /app
#WORKDIR /app

# Create a Python virtual environment
#USER root
#RUN apt-get update && apt-get install -y python3-venv && rm -rf /var/lib/apt/lists/*
#RUN python3 -m venv venv

# Activate the virtual environment
#ENV VIRTUAL_ENV=/app/venv
#ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Unset the PIP_USER environment variable
#ENV PIP_USER=0

# Copy the current directory contents into the container at /app
#COPY . /app

# Switch to airflow user
#USER airflow

# Upgrade pip
#RUN pip install --upgrade pip

# Install any needed packages specified in requirements.txt
#COPY requirements.txt /app
#RUN pip install --no-cache-dir -r requirements.txt

# Define the entry point and command for the Docker container
#ENTRYPOINT ["airflow"]
#CMD ["webserver"]