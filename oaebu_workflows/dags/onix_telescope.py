# Copyright 2021 Curtin University
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

# Author: James Diprose

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from oaebu_workflows.identifiers import WorkflowTypes
from oaebu_workflows.workflows.onix_telescope import OnixTelescope
from observatory.platform.utils.api import make_observatory_api

# Fetch all ONIX workflows
api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=WorkflowTypes.onix)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)

# Make all ONIX telescopes
for workflow in workflows:
    organisation = workflow.organisation
    organisation_name = organisation.name
    project_id = organisation.project_id
    download_bucket = organisation.download_bucket
    transform_bucket = organisation.transform_bucket
    dataset_location = "us"
    date_regex = workflow.extra.get("date_regex")
    date_format = workflow.extra.get("date_format")

    workflow = OnixTelescope(
        organisation_name=organisation_name,
        project_id=project_id,
        download_bucket=download_bucket,
        transform_bucket=transform_bucket,
        dataset_location=dataset_location,
        date_regex=date_regex,
        date_format=date_format,
        workflow_id=workflow.id,
    )
    globals()[workflow.dag_id] = workflow.make_dag()
