FROM quay.io/astronomer/astro-runtime:13.6.0

# Root user for installations
USER root 

# Install git
RUN apt-get update && apt-get install git -y

USER astro

# Install Observatory Platform
RUN git clone https://github.com/The-Academic-Observatory/observatory-platform.git && \
    pip install ./observatory-platform --constraint  https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.10.txt
