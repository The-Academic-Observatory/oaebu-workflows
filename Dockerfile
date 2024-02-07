FROM quay.io/astronomer/astro-runtime:9.10.0

# The following describes what various dependencies are used for. Some dependencies are specified in packages.txt
# and some are installed in this Dockerfile if not available in apt by default.
#
# KubernetesPodOperator:
#   * google-cloud-cli and google-cloud-cli-gke-gcloud-auth-plugin.
#   * apt-transport-https, ca-certificates, gnupg, curl, sudo are installed as dependencies the above packages
# OpenAlex: google-cloud-cli
# Crossref Metadata: pigz
# Open Citations: unzip
# ORCID: s5cmd

# Install custom dependencies for DAGs that are not available via apt by default
USER root

# To connect to GKE with KubernetesPodOperator install google-cloud-cli and google-cloud-cli-gke-gcloud-auth-plugin
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.asc
RUN apt-get update && apt-get install -y google-cloud-cli google-cloud-cli-gke-gcloud-auth-plugin

# Install git
RUN apt-get install git -y

USER astro

# Install Observatory Platform
RUN git clone --branch feature/astro https://github.com/The-Academic-Observatory/observatory-platform.git
RUN pip install -e ./observatory-platform/observatory-api --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-no-providers-3.10.txt
RUN pip install -e ./observatory-platform/observatory-platform --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-no-providers-3.10.txt

# Install Academic Observatory Workflows
COPY oaebu-workflows ./oaebu-workflows
RUN pip install -e ./oaebu-workflows --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-no-providers-3.10.txt