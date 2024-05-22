# Copyright 2020-2024 Curtin University
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
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat, Client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_chain,
    wait_fixed,
    retry_if_exception_type,
)

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from oaebu_workflows.config import schema_folder, oaebu_user_agent_header
from oaebu_workflows.onix_utils import OnixTransformer
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.google.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path, gcs_download_blob
from observatory_platform.google.bigquery import bq_load_table, bq_sharded_table_id, bq_create_dataset
from observatory_platform.airflow.release import SnapshotRelease, set_task_state, make_snapshot_date
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.airflow import on_failure_callback


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
        self.download_file_name = f"metadata_{snapshot_date.format('YYYYMMDD')}.xml"
        self.transform_file_name = "transformed.jsonl.gz"  # Final onix file

    @property
    def download_path(self):
        return os.path.join(self.download_folder, self.download_file_name)

    @property
    def transform_path(self):
        return os.path.join(self.transform_folder, self.transform_file_name)

    @property
    def transform_files(self):
        files = os.listdir(self.transform_folder)
        return [os.path.join(self.transform_folder, f) for f in files]

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.download_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return OapenMetadataRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            snapshot_date=pendulum.from_format(dict_["snapshot_date"], "YYYY-MM-DD"),
        )

    def to_dict(self) -> dict:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "snapshot_date": self.snapshot_date.to_date_string(),
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    metadata_uri: str,
    metadata_partner: Union[str, OaebuPartner] = "oapen_metadata",
    elevate_related_products: bool = False,
    bq_dataset_description: str = "OAPEN Metadata converted to ONIX",
    bq_table_description: str = None,
    api_dataset_id: str = "dataset_api",
    api_identifier: str = "oapen_metadata",
    catchup: bool = False,
    start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
    schedule: str = "0 12 * * Sun",  # Midday every sunday
    max_active_runs: int = 1,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """Construct a OapenMetadata DAG.
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param metadata_uri: The URI of the metadata XML file
    :param metadata_partner: The metadata partner name
    :param elevate_related_products: Whether to pull out the related products to the product level.
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_dataset_id: The name of the Bigquery dataset to store the API release(s)
    :param catchup: Whether to catchup the DAG or not
    :param start_date: The start date of the DAG
    :param schedule: The schedule interval of the DAG
    :param max_active_runs: The maximum number of active DAG runs
    :param retries: The number of times to retry failed tasks
    :param retry_delay: The delay between retries in minutes
    """

    metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)

    # Fixture file paths
    oapen_schema = os.path.join(schema_folder(workflow_module="oapen_metadata_telescope"), "oapen_metadata_filter.json")

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["oaebu"],
        max_active_runs=max_active_runs,
        default_args=dict(
            retries=retries, retry_delay=pendulum.duration(minutes=retry_delay), on_failure_callback=on_failure_callback
        ),
    )
    def oapen_metadata():
        @task()
        def make_release(**context) -> dict:
            """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
            called in 'task_callable'.

            :param context: the context passed from the PythonOperator.
            See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
            :return: The Oapen metadata release instance"""
            snapshot_date = make_snapshot_date(**context)
            return OapenMetadataRelease(dag_id, context["run_id"], snapshot_date).to_dict()

        @task()
        def download(release: dict, **context) -> None:
            """Task to download the OapenMetadataRelease release.

            :param context: the context passed from the PythonOperator.
            :param release: an OapenMetadataRelease instance.
            """

            release = OapenMetadataRelease.from_dict(release)
            logging.info(f"Downloading metadata XML from url: {metadata_uri}")
            download_metadata(metadata_uri, release.download_path)
            success = gcs_upload_files(
                bucket_name=cloud_workspace.download_bucket,
                file_paths=[release.download_path],
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task(queue="queue-a10")
        def transform(release: dict, **context) -> None:
            """Transform the oapen metadata XML file into a valid ONIX file. This task runs on a different queue
            because it uses more memory than other tasks."""

            release = OapenMetadataRelease.from_dict(release)
            # Download files from GCS
            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.download_blob_name,
                file_path=release.download_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_blob_name}")

            # Parse the downloaded metadata through the schema to extract relevant fields only
            transformer = OnixTransformer(
                input_path=release.download_path,
                output_dir=release.transform_folder,
                filter_products=True,
                error_removal=True,
                normalise_related_products=True,
                deduplicate_related_products=True,
                elevate_related_products=elevate_related_products,
                add_name_fields=True,
                collapse_subjects=True,
                filter_schema=oapen_schema,
                keep_intermediate=True,
            )
            out_file = transformer.transform()
            if release.transform_path != out_file:
                raise FileNotFoundError(
                    f"Expected file {release.transform_path} not equal to transformed file: {out_file}"
                )

            success = gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket,
                file_paths=release.transform_files,
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task()
        def bq_load(release: dict, **context) -> None:
            """Load the transformed ONIX file into bigquery"""

            release = OapenMetadataRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=metadata_partner.bq_dataset_id,
                location=cloud_workspace.data_location,
                description=bq_dataset_description,
            )
            uri = gcs_blob_uri(
                cloud_workspace.transform_bucket,
                gcs_blob_name_from_path(release.transform_path),
            )
            table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                metadata_partner.bq_dataset_id,
                metadata_partner.bq_table_name,
                release.snapshot_date,
            )
            client = Client(project=cloud_workspace.project_id)
            state = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=metadata_partner.schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                table_description=bq_table_description,
                client=client,
            )
            set_task_state(state, context["ti"].task_id, release=release)

        @task()
        def add_new_dataset_releases(release: dict, **context) -> None:
            """Adds release information to API."""

            release = OapenMetadataRelease.from_dict(release)
            client = Client(project=cloud_workspace.project_id)
            api = DatasetAPI(project_id=cloud_workspace.project_id, dataset_id=api_dataset_id, client=client)
            api.seed_db()
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_identifier,
                dag_run_id=release.run_id,
                created=pendulum.now(),
                modified=pendulum.now(),
                snapshot_date=release.snapshot_date,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )
            api.add_dataset_release(dataset_release)

        @task()
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            release = OapenMetadataRelease.from_dict(release)
            cleanup(
                dag_id=dag_id,
                execution_date=context["execution_date"],
                workflow_folder=release.workflow_folder,
            )

        task_check_dependencies = check_dependencies()
        xcom_release = make_release()
        task_download = download(xcom_release)
        task_transform = transform(xcom_release)
        task_bq_load = bq_load(xcom_release)
        task_add_release = add_new_dataset_releases(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> xcom_release
            >> task_download
            >> task_transform
            >> task_bq_load
            >> task_add_release
            >> task_cleanup_workflow
        )

    return oapen_metadata()


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
    response = requests.get(uri, headers=oaebu_user_agent_header())
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
