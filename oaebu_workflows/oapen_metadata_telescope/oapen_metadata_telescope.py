# Copyright 2020-2023 Curtin University
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

# Author: Aniek Roelofs, Keegan Smith

from __future__ import annotations

import logging
import os
import requests
import xmltodict
from xml.parsers.expat import ExpatError
from typing import Union

import pendulum
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat
from tenacity import (
    retry,
    stop_after_attempt,
    wait_chain,
    wait_fixed,
    retry_if_exception_type,
)

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from oaebu_workflows.config import schema_folder
from oaebu_workflows.onix_utils import OnixTransformer
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.utils.url_utils import get_user_agent
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.gcs import (
    gcs_upload_files,
    gcs_blob_uri,
    gcs_blob_name_from_path,
)
from observatory.platform.bigquery import bq_load_table, bq_sharded_table_id, bq_create_dataset
from observatory.platform.workflows.workflow import (
    SnapshotRelease,
    Workflow,
    make_snapshot_date,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


# Download job will wait 120 seconds between first 2 attempts, then 30 minutes for the following 3
DOWNLOAD_RETRY_CHAIN = wait_chain(*[wait_fixed(120) for _ in range(2)] + [wait_fixed(1800) for _ in range(3)])


class OapenMetadataRelease(SnapshotRelease):
    def __init__(self, dag_id: str, run_id: str, snapshot_date: pendulum.DateTime):
        """Construct a OapenMetadataRelease instance

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param snapshot_date: The date of the snapshot_date/release
        """
        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.download_path = os.path.join(self.download_folder, f"metadata_{snapshot_date.format('YYYYMMDD')}.xml")
        self.transform_path = os.path.join(self.transform_folder, "transformed.jsonl.gz")  # Final onix file

    @property
    def transform_files(self):
        files = os.listdir(self.transform_folder)
        return [os.path.join(self.transform_folder, f) for f in files]


class OapenMetadataTelescope(Workflow):
    """Oapen Metadata Telescope"""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        metadata_uri: str,
        metadata_partner: Union[str, OaebuPartner] = "oapen_metadata",
        elevate_related_products: bool = False,
        bq_dataset_id: str = "onix",
        bq_table_name: str = "onix",
        bq_dataset_description: str = "OAPEN Metadata converted to ONIX",
        bq_table_description: str = None,
        api_dataset_id: str = "oapen",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        catchup: bool = False,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule: str = "0 12 * * Sun",  # Midday every sunday
    ):
        """Construct a OapenMetadataTelescope instance.
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param metadata_uri: The URI of the metadata XML file
        :param metadata_partner: The metadata partner name
        :param elevate_related_products: Whether to pull out the related products to the product level.
        :param bq_dataset_id: The BigQuery dataset ID
        :param bq_table_name: The BigQuery table name
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param catchup: Whether to catchup the DAG or not
        :param start_date: The start date of the DAG
        :param schedule: The schedule interval of the DAG
        """
        super().__init__(
            dag_id,
            start_date,
            schedule,
            airflow_conns=[observatory_api_conn_id],
            catchup=catchup,
            tags=["oaebu"],
        )
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.metadata_uri = metadata_uri
        self.metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
        self.elevate_related_products = elevate_related_products
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.observatory_api_conn_id = observatory_api_conn_id

        # Fixture file paths
        self.oapen_schema = os.path.join(
            schema_folder(workflow_module="oapen_metadata_telescope"), "oapen_metadata_filter.json"
        )

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> OapenMetadataRelease:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: The Oapen metadata release instance"""
        snapshot_date = make_snapshot_date(**kwargs)
        release = OapenMetadataRelease(self.dag_id, kwargs["run_id"], snapshot_date)
        return release

    def download(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Task to download the OapenMetadataRelease release.

        :param kwargs: the context passed from the PythonOperator.
        :param release: an OapenMetadataRelease instance.
        """
        logging.info(f"Downloading metadata XML from url: {self.metadata_uri}")
        download_metadata(self.metadata_uri, release.download_path)

    def upload_downloaded(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Task to upload the downloaded OAPEN metadata"""
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket,
            file_paths=[release.download_path],
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Transform the oapen metadata XML file into a valid ONIX file"""
        # Parse the downloaded metadata through the schema to extract relevant fields only
        transformer = OnixTransformer(
            input_path=release.download_path,
            output_dir=release.transform_folder,
            filter_products=True,
            error_removal=True,
            normalise_related_products=True,
            deduplicate_related_products=True,
            elevate_related_products=self.elevate_related_products,
            add_name_fields=True,
            collapse_subjects=True,
            filter_schema=self.oapen_schema,
            keep_intermediate=True,
        )
        out_file = transformer.transform()
        if release.transform_path != out_file:
            raise FileNotFoundError(f"Expected file {release.transform_path} not equal to transformed file: {out_file}")

    def upload_transformed(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Task to upload the transformed OAPEN metadata"""
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket,
            file_paths=release.transform_files,
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Load the transformed ONIX file into bigquery"""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.metadata_partner.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )
        uri = gcs_blob_uri(
            self.cloud_workspace.transform_bucket,
            gcs_blob_name_from_path(release.transform_path),
        )
        table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.snapshot_date,
        )
        state = bq_load_table(
            uri=uri,
            table_id=table_id,
            schema_file_path=self.metadata_partner.schema_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            table_description=self.bq_table_description,
        )
        set_task_state(state, kwargs["ti"].task_id, release=release)

    def add_new_dataset_releases(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        cleanup(
            dag_id=self.dag_id,
            execution_date=kwargs["execution_date"],
            workflow_folder=release.workflow_folder,
        )


@retry(
    stop=stop_after_attempt(5),
    wait=DOWNLOAD_RETRY_CHAIN,
    retry=retry_if_exception_type((ExpatError, ConnectionError, AirflowException)),
    reraise=True,
)
def download_metadata(uri: str, download_path: str) -> None:
    """Downloads the OAPEN metadata XML file
    OAPEN's downloader can give an incomplete file if the metadata is partially generated.
    In this scenario, we should wait until the metadata generator has finished.
    Otherwise, an attempt to parse the data will result in an XML ParseError.
    Another scenario is that OAPEN returns only a header in the XML. We want this to also raise an error.
    OAPEN metadata generation can take over an hour

    :param uri: the url to query for the metadata
    :param download_path: filepath to store te downloaded file
    :raises ConnectionError: raised if the response from the metadata server does not have code 200
    :raises AirflowException: raised if the response does not contain any Product fields
    """
    headers = {"User-Agent": f"{get_user_agent(package_name='oaebu_workflows')}"}
    response = requests.get(uri, headers=headers)
    if response.status_code != 200:
        raise ConnectionError(f"Expected status code 200 from url {uri}, instead got response: {response.text}")
    with open(download_path, "w") as f:
        f.write(response.content.decode("utf-8"))
    logging.info(f"Successfully downloadeded XML to {download_path}")

    # Attempt to parse the XML, will raise an ExpatError if it's invalid
    with open(download_path, "rb") as f:
        xmltodict.parse(f)
    logging.info("XML file is valid")

    # Check that more than just the header is returned
    if "<Product>" not in response.content.decode("utf-8"):
        raise AirflowException("No products found in metadata")
