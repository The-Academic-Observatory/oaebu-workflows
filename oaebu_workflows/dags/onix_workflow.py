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

# Author: Tuan Chien & Richard Hosking

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from typing import List, Tuple

from airflow.exceptions import AirflowException
from oaebu_workflows.identifiers import WorkflowTypes
from oaebu_workflows.workflows.oaebu_partners import OaebuPartner
from oaebu_workflows.workflows.onix_workflow import OnixWorkflow
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.workflow import Workflow
from observatory.platform.utils.api import make_observatory_api
import json


def is_oaebu_telescope(workflow: Workflow) -> bool:
    """Determine whether a workflow is an OAEBU telescope.

    :param workflow: workflow to check.
    :return: Whether the workflow is used for OAEBU.
    """

    if workflow.tags is None:
        return False

    tags = json.loads(workflow.tags)
    return "oaebu" in tags


def get_gcp_address(dataset: Dataset) -> Tuple[str, str, str]:
    """Get the project id, dataset id, and table id from a GCP storage location.

    :param dataset: Dataset info.
    :return: project id, dataset id, table id.
    """

    if dataset.service != "bigquery":
        raise AirflowException("Unsupported DatasetStorage type")

    return dataset.address.split(".")


def get_isbn_field_name(dataset: Dataset) -> str:
    """Get the ISBN field name in a dataset.

    :param dataset: Dataset info.
    :return: ISBN field name.
    """

    if dataset.dataset_type.extra is None:
        return False

    isbn_field_name = dataset.dataset_type.extra.get("isbn_field_name", None)
    if isbn_field_name is None:
        raise AirflowException("isbn_field_name missing from Dataset extra")

    return isbn_field_name


def get_title_field_name(dataset: Dataset) -> str:
    """Get the title field name in a dataset.

    :param dataset: Dataset info.
    :return: Title field name.
    """

    if dataset.dataset_type.extra is None:
        return False

    title_field_name = dataset.dataset_type.extra.get("title_field_name", None)
    if title_field_name is None:
        raise AirflowException("title_field_name missing from Dataset extra")

    return title_field_name


def is_sharded(dataset: Dataset) -> bool:
    """Determine if a dataset is sharded.

    :param dataset: Dataset info.
    :return: Whether the table is sharded.
    """

    return dataset.dataset_type.table_type.type_id == "sharded"


def get_oaebu_partner_data(organisation_id: int) -> List[OaebuPartner]:
    """Get the OAEBU datasets for a given organisation.

    :param organisation_id: Organisation id.
    :return: List of OAEBU partner dataset information.
    """

    workflows = api.get_workflows(organisation_id=organisation_id, limit=1000)
    partners = list()

    for workflow in workflows:
        if not is_oaebu_telescope(workflow):
            continue

        datasets = api.get_datasets(workflow_id=workflow.id, limit=1000)
        for dataset in datasets:
            gcp_project_id, gcp_dataset_id, gcp_table_id = get_gcp_address(dataset)

            partners.append(
                OaebuPartner(
                    name=dataset.dataset_type.type_id,
                    dag_id_prefix=workflow.workflow_type.type_id,
                    gcp_project_id=gcp_project_id,
                    gcp_dataset_id=gcp_dataset_id,
                    gcp_table_id=gcp_table_id,
                    isbn_field_name=get_isbn_field_name(dataset),
                    title_field_name=get_title_field_name(dataset),
                    sharded=is_sharded(dataset),
                )
            )

    return partners


# Fetch all ONIX telescopes
api = make_observatory_api()
workflow_type = api.get_workflow_type(type_id=WorkflowTypes.onix)
workflows = api.get_workflows(workflow_type_id=workflow_type.id, limit=1000)

# Create workflows for each organisation
for workflow in workflows:
    org_name = workflow.organisation.name
    gcp_project_id = workflow.organisation.gcp_project_id
    gcp_bucket_name = workflow.organisation.gcp_transform_bucket
    data_partners = get_oaebu_partner_data(workflow.organisation.id)

    workflow = OnixWorkflow(
        org_name=org_name,
        gcp_project_id=gcp_project_id,
        gcp_bucket_name=gcp_bucket_name,
        data_partners=data_partners,
        workflow_id=workflow.id,
    )

    globals()[workflow.dag_id] = workflow.make_dag()
