FROM quay.io/astronomer/astro-runtime:10.5.0

# Root user for installations
USER root 

# Install git
RUN apt-get update && apt-get install git -y

USER astro

# Install Observatory Platform
RUN git clone --branch feature/astro-refactor https://github.com/The-Academic-Observatory/observatory-platform.git
RUN pip install -e ./observatory-platform/ --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.3/constraints-no-providers-3.10.txt