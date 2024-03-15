# Copyright 2023-2024 Curtin University
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

import os
import logging
from typing import Union

import pendulum
from pendulum.datetime import DateTime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat, Client

from oaebu_workflows.onix_utils import OnixTransformer
from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.google.bigquery import bq_load_table, bq_sharded_table_id, bq_create_dataset
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.url_utils import retry_get_url
from observatory_platform.google.gcs import gcs_upload_files, gcs_blob_name_from_path, gcs_blob_uri, gcs_download_blob
from observatory_platform.airflow.release import SnapshotRelease, set_task_state, make_snapshot_date
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.airflow import on_failure_callback


THOTH_URL = "{host_name}/specifications/{format_specification}/publisher/{publisher_id}"
DEFAULT_HOST_NAME = "https://export.thoth.pub"


class ThothRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: DateTime,
    ):
        """Construct a ThothRelease.
        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param snapshot_date: The date of the snapshot_date/release
        """
        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.download_file_name = f"thoth_{snapshot_date.format('YYYY_MM_DD')}.xml"
        self.transform_file_name = "transformed.jsonl.gz"

    @property
    def download_path(self) -> str:
        return os.path.join(self.download_folder, self.download_file_name)

    @property
    def transform_path(self) -> str:
        return os.path.join(self.transform_folder, self.transform_file_name)

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.download_path)

    @property
    def tranform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return ThothRelease(
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
    publisher_id: str,
    format_specification: str,
    elevate_related_products: bool = False,
    metadata_partner: Union[str, OaebuPartner] = "thoth",
    bq_dataset_description: str = "Thoth ONIX Feed",
    bq_table_description: str = "Thoth ONIX Feed",
    api_dataset_id: str = "onix",
    catchup: bool = False,
    start_date: DateTime = pendulum.datetime(2022, 12, 1),
    schedule: str = "0 12 * * Sun",  # Midday every sunday
    max_active_runs: int = 1,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """Construct an Thoth DAG.
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param publisher_id: The Thoth ID for this publisher
    :param format_specification: The Thoth ONIX/metadata format specification. e.g. "onix_3.0::oapen"
    :param elevate_related_products: Whether to pull out the related products to the product level.
    :param metadata_partner: The metadata partner name
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_dataset_id: The ID to store the dataset release in the API
    :param catchup: Whether to catchup the DAG or not
    :param start_date: The start date of the DAG
    :param schedule: The schedule interval of the DAG
    :param max_active_runs: The maximum number of active DAG runs
    :param retries: The number of times to retry failed tasks
    :param retry_delay: The delay between retries in minutes
    """

    metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        tags=["oaebu"],
        max_active_runs=max_active_runs,
        default_args=dict(
            retries=retries, retry_delay=pendulum.duration(minutes=retry_delay), on_failure_callback=on_failure_callback
        ),
    )
    def thoth_telescope():
        @task()
        def make_release(**content) -> dict:
            """Creates a new Thoth release instance

            :param content: the context passed from the PythonOperator.
            See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
            :return: The Thoth release instance
            """

            snapshot_date = make_snapshot_date(**content)
            return ThothRelease(dag_id=dag_id, run_id=content["run_id"], snapshot_date=snapshot_date).to_dict()

        @task()
        def download(release: dict, **content) -> None:
            """Task to download the ONIX release from Thoth.

            :param release: The Thoth release instance
            """

            release = ThothRelease.from_dict(release)
            thoth_download_onix(
                publisher_id=publisher_id,
                format_spec=format_specification,
                download_path=release.download_path,
            )
            success = gcs_upload_files(bucket_name=cloud_workspace.download_bucket, file_paths=[release.download_path])
            set_task_state(success, content["ti"].task_id, release=release)

        @task()
        def transform(release: dict, **content) -> None:
            """Task to transform the Thoth ONIX data"""

            release = ThothRelease.from_dict(release)
            # Download files from GCS
            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.download_blob_name,
                file_path=release.download_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_blob_name}")

            transformer = OnixTransformer(
                input_path=release.download_path,
                output_dir=release.transform_folder,
                deduplicate_related_products=elevate_related_products,
                elevate_related_products=elevate_related_products,
                add_name_fields=True,
                collapse_subjects=True,
            )
            out_file = transformer.transform()
            if release.transform_path != out_file:
                raise FileNotFoundError(
                    f"Expected file {release.transform_path} not equal to transformed file: {out_file}"
                )
            success = gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(success, content["ti"].task_id, release=release)

        @task()
        def bq_load(release: dict, **content) -> None:
            """Task to load the transformed ONIX jsonl file to BigQuery."""

            release = ThothRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=metadata_partner.bq_dataset_id,
                location=cloud_workspace.data_location,
                description=bq_dataset_description,
            )
            uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
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
            set_task_state(state, content["ti"].task_id, release=release)

        @task()
        def add_new_dataset_releases(release: dict, **content) -> None:
            """Adds release information to API."""

            release = ThothRelease.from_dict(release)
            client = Client(project=cloud_workspace.project_id)
            api = DatasetAPI(project_id=cloud_workspace.project_id, dataset_id=api_dataset_id, client=client)
            api.seed_db()
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_dataset_id,
                dag_run_id=release.run_id,
                created=pendulum.now(),
                modified=pendulum.now(),
                snapshot_date=release.snapshot_date,
                data_interval_start=content["data_interval_start"],
                data_interval_end=content["data_interval_end"],
            )
            api.add_dataset_release(dataset_release)

        @task()
        def cleanup_workflow(release: dict, **content) -> None:
            """Delete all files, folders and XComs associated with this release."""

            release = ThothRelease.from_dict(release)
            cleanup(dag_id=dag_id, execution_date=content["execution_date"], workflow_folder=release.workflow_folder)

        task_check_dependencies = check_dependencies()
        xcom_release = make_release()
        task_download = download(xcom_release)
        task_transform = transform(xcom_release)
        task_bq_load = bq_load(xcom_release)
        task_add_new_dataset_releases = add_new_dataset_releases(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> xcom_release
            >> task_download
            >> task_transform
            >> task_bq_load
            >> task_add_new_dataset_releases
            >> task_cleanup_workflow
        )

    return thoth_telescope()


def thoth_download_onix(
    publisher_id: str,
    download_path: str,
    format_spec: str,
    host_name: str = DEFAULT_HOST_NAME,
    num_retries: int = 3,
) -> None:
    """Hits the Thoth API and requests the ONIX feed for a particular publisher.
    Creates a file called onix.xml at the specified location

    :param publisher_id: The ID of the publisher. Can be found using Thoth GraphiQL API
    :param download_path: The path to download ONIX the file to
    :param format_spec: The ONIX format specification to use. Options can be found with the /formats endpoint of the API
    :param host_name: The Thoth host URL
    :param num_retries: The number of times to retry the download, given an unsuccessful return code
    """
    url = THOTH_URL.format(host_name=host_name, format_specification=format_spec, publisher_id=publisher_id)
    logging.info(f"Downloading ONIX XML from {url}")
    response = retry_get_url(url, num_retries=num_retries)
    if response.status_code != 200:
        raise AirflowException(
            f"Request for URL {url} was unsuccessful with code: {response.status_code}\nContent response: {response.content.decode('utf-8')}"
        )
    with open(download_path, "wb") as f:
        f.write(response.content)
