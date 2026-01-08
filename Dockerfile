# FROM quay.io/astronomer/astro-runtime:3.1-9
FROM astrocrpublic.azurecr.io/runtime:3.1-9

# Root user for installations
USER root 

# Install git
RUN apt-get update && apt-get install git -y

USER astro

# Install Observatory Platform
RUN git clone --branch airflow3 https://github.com/The-Academic-Observatory/observatory-platform.git && \
    pip install ./observatory-platform/ --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.12.txt && \
    pip install . --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.12.txt 
 
