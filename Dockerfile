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

# Install git
RUN apt-get update && apt-get install git -y

USER astro

# Install Observatory Platform
RUN git clone --branch feature/astro-refactor https://github.com/The-Academic-Observatory/observatory-platform.git
RUN pip install -e ./observatory-platform/ --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-no-providers-3.10.txt