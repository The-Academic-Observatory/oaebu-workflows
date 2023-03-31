# Copyright 2020 Curtin University
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

from oaebu_workflows.identifiers import WorkflowTypes
from oaebu_workflows.workflows.fulcrum_telescope import FulcrumTelescope
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.workflow_utils import make_dag_id

# Fetch all workflows
api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=WorkflowTypes.fulcrum)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)

# Make all workflows
for workflow in workflows:
    organisation = workflow.organisation
    organisation_name = organisation.name
    project_id = organisation.project_id
    download_bucket = organisation.download_bucket
    transform_bucket = organisation.transform_bucket
    data_location = "us"
    dag_id = make_dag_id(FulcrumTelescope.DAG_ID_PREFIX, organisation_name)

    workflow = FulcrumTelescope(
        workflow_id=workflow.id,
        organisation_name=organisation_name,
        project_id=project_id,
        download_bucket=download_bucket,
        transform_bucket=transform_bucket,
        data_location=data_location,
        dag_id=dag_id,
        catchup=True,
    )
    globals()[workflow.dag_id] = workflow.make_dag()
