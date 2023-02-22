# Copyright 2023 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Keegan Smith

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

import pendulum
from google.cloud.bigquery import SourceFormat

from oaebu_workflows.workflows.thoth_telescope import ThothTelescope
from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.workflow_utils import make_dag_id

api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=ThothTelescope.DAG_ID_PREFIX)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)

# Create dags for each organisation
for workflow in workflows:
    dag_id = make_dag_id(ThothTelescope.DAG_ID_PREFIX, workflow.organisation.name)
    organisation = workflow.organisation
    organisation_name = organisation.name
    project_id = organisation.project_id
    download_bucket = organisation.download_bucket
    transform_bucket = organisation.transform_bucket
    data_location = "us"
    dataset_description = f"{organisation_name} ONIX feed from Thoth"
    publisher_id = workflow.extra.get("publisher_id")

    workflow = ThothTelescope(
        dag_id=dag_id,
        project_id=project_id,
        publisher_id=publisher_id,
        download_bucket=download_bucket,
        transform_bucket=transform_bucket,
        data_location=data_location,
        workflow_id=workflow.id,
        start_date=pendulum.datetime(2022, 12, 1),
        schedule_interval="@weekly",
        dataset_id="onix",
        schema_folder=default_schema_folder(),
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        catchup=False,
        format_specification="onix_3.0::oapen",
        dataset_description=dataset_description,
    )

    globals()[workflow.dag_id] = workflow.make_dag()
