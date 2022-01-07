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
from oaebu_workflows.identifiers import TelescopeTypes
from oaebu_workflows.workflows.oaebu_partners import OaebuPartner
from oaebu_workflows.workflows.onix_workflow import OnixWorkflow
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.api.client.model.dataset_storage import DatasetStorage
from observatory.api.client.model.telescope import Telescope
from observatory.platform.utils.api import make_observatory_api


def is_oaebu_telescope(telescope: Telescope) -> bool:
    """Determine whether a telescope is an OAEBU telescope.

    :param telescope: Telescope to check.
    :return: Whether the telescope is used for OAEBU.
    """

    if telescope.extra is None:
        return False

    groups = telescope.extra.get("groups", None)
    if groups is None or "oaebu" not in groups:
        return False
    return True


def get_gcp_address(storage: DatasetStorage) -> Tuple[str, str, str]:
    """Get the project id, dataset id, and table id from a GCP storage location.

    :param storage: Dataset storage info.
    :return: project id, dataset id, table id.
    """

    if storage.service != "google":
        raise AirflowException("Unsupported DatasetStorage type")

    return storage.address.split(".")


def get_isbn_field_name(dataset: Dataset) -> str:
    """Get the ISBN field name in a dataset.

    :param dataset: Dataset info.
    :return: ISBN field name.
    """

    if dataset.extra is None:
        return False

    isbn_field_name = dataset.extra.get("isbn_field_name", None)
    if isbn_field_name is None:
        raise AirflowException("isbn_field_name missing from Dataset extra")

    return isbn_field_name


def get_title_field_name(dataset: Dataset) -> str:
    """Get the title field name in a dataset.

    :param dataset: Dataset info.
    :return: Title field name.
    """

    if dataset.extra is None:
        return False

    title_field_name = dataset.extra.get("title_field_name", None)
    if title_field_name is None:
        raise AirflowException("title_field_name missing from Dataset extra")

    return title_field_name


def is_sharded(storage: DatasetStorage) -> bool:
    """Determine if a dataset storage table is sharded.

    :param storage: Dataset storage info.
    :return: Whether the table is sharded.
    """

    if storage.extra is None:
        return False

    table_type = storage.extra.get("table_type", None)
    if table_type is None:
        raise AirflowException("DatasetStorage table_type not found")

    return table_type == "sharded"


def get_oaebu_partner_data(organisation_id: int) -> List[OaebuPartner]:
    """Get the OAEBU datasets for a given organisation.

    :param organisation_id: Organisation id.
    :return: List of OAEBU partner dataset information.
    """

    telescopes = api.get_telescopes(organisation_id=organisation_id, limit=1000)
    partners = list()

    for telescope in telescopes:
        if not is_oaebu_telescope(telescope):
            continue

        datasets = api.get_datasets(telescope_id=telescope.id, limit=1000)
        for dataset in datasets:
            storages = api.get_dataset_storages(dataset_id=dataset.id, limit=1000)
            for storage in storages:
                gcp_project_id, gcp_dataset_id, gcp_table_id = get_gcp_address(storage)

                partners.append(
                    OaebuPartner(
                        name=dataset.name,
                        dag_id_prefix=telescope.telescope_type.type_id,
                        gcp_project_id=gcp_project_id,
                        gcp_dataset_id=gcp_dataset_id,
                        gcp_table_id=gcp_table_id,
                        isbn_field_name=get_isbn_field_name(dataset),
                        title_field_name=get_title_field_name(dataset),
                        sharded=is_sharded(storage),
                    )
                )

    return partners


# Fetch all ONIX telescopes
api = make_observatory_api()
telescope_type = api.get_telescope_type(type_id=TelescopeTypes.onix)
telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)

# Create workflows for each organisation
for telescope in telescopes:
    org_name = telescope.organisation.name
    gcp_project_id = telescope.organisation.gcp_project_id
    gcp_bucket_name = telescope.organisation.gcp_transform_bucket
    data_partners = get_oaebu_partner_data(telescope.organisation.id)

    workflow = OnixWorkflow(
        org_name=org_name,
        gcp_project_id=gcp_project_id,
        gcp_bucket_name=gcp_bucket_name,
        data_partners=data_partners,
    )

    globals()[workflow.dag_id] = workflow.make_dag()
