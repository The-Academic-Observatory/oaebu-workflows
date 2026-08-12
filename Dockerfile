FROM astrocrpublic.azurecr.io/runtime:3.2-6

# Root user for installations
USER root 

# Install git
RUN apt-get update && apt-get install git -y

USER astro

# Install Observatory Platform
# TODO: remove airflow_3 branch when observatory_platform is merged
RUN git clone -b airflow_3 https://github.com/The-Academic-Observatory/observatory-platform.git && \
    pip install ./observatory-platform --constraint  https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt
