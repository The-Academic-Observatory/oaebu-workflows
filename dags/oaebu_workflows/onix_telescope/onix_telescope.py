# Copyright 2021-2024 Curtin University
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

# Author: James Diprose, Keegan Smith

import logging
import os
import re
import shutil
from typing import List, Union

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowSkipException
from google.cloud.bigquery import Client, SourceFormat

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from oaebu_workflows.onix_utils import OnixTransformer
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.release import set_task_state, SnapshotRelease
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import cleanup, CloudWorkspace
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.google.bigquery import bq_create_dataset, bq_load_table, bq_sharded_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_download_blob, gcs_upload_files
from observatory_platform.sftp import make_sftp_connection, SftpFolders


class OnixRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        onix_file_name: str,
    ):
        """Construct an OnixRelease.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param snapshot_date: The date of the snapshot/release
        :param onix_file_name: The ONIX file name.
        """
        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.onix_file_name = onix_file_name
        self.download_file_name = self.onix_file_name
        self.parsed_file_name = "full.jsonl"
        self.transform_file_name = "onix.jsonl.gz"

    @property
    def download_path(self):
        return os.path.join(self.download_folder, self.download_file_name)

    @property
    def parsed_path(self):
        return os.path.join(self.transform_folder, self.parsed_file_name)

    @property
    def transform_path(self):
        return os.path.join(self.transform_folder, self.transform_file_name)

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.download_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return OnixRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            snapshot_date=pendulum.from_format(dict_["snapshot_date"], "YYYY-MM-DD"),
            onix_file_name=dict_["onix_file_name"],
        )

    def to_dict(self) -> dict:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "snapshot_date": self.snapshot_date.to_date_string(),
            "onix_file_name": self.onix_file_name,
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    date_regex: str,
    sftp_root: str = "/",
    metadata_partner: Union[str, OaebuPartner] = "onix",
    elevate_related_products: bool = False,
    bq_dataset_description: str = "ONIX data provided by Org",
    bq_table_description: str = None,
    api_bq_dataset_id: str = "dataset_api",
    sftp_service_conn_id: str = "sftp_service",
    catchup: bool = False,
    schedule: str = "0 12 * * *",  # Midday everyday
    start_date: pendulum.DateTime = pendulum.datetime(2021, 3, 28),
    max_active_runs: int = 1,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """Construct an OINX DAG.
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param sftp_root: The working root of the SFTP server, passed to the SftoFolders class
    :param metadata_partner: The metadata partner name
    :param elevate_related_products: Whether to pull out the related products into their own product field
    :param date_regex: Regular expression for extracting a date string from an ONIX file name
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_bq_dataset_id: The name of the Bigquery dataset to store the API release(s)
    :param sftp_service_conn_id: Airflow connection ID for the SFTP service
    :param catchup: Whether to catchup the DAG or not
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    :param max_active_runs: The maximum number of active DAG runs.
    :param retries: The number of times to retry failed tasks.
    :param retry_delay: The delay between retries in minutes.
    """
    metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
    sftp_folders = SftpFolders(dag_id, sftp_conn_id=sftp_service_conn_id, sftp_root=sftp_root)

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
    def onix_telescope():
        @task()
        def fetch_releases(**context) -> List[dict]:
            """Lists all ONIX releases and publishes their file names as an XCom.

            :param context: the context passed from the BranchPythonOperator.
            See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
            :return: the identifier of the task to execute next.
            """

            # List release dates
            release_info = []
            with make_sftp_connection(sftp_service_conn_id) as sftp:
                files = sftp.listdir(sftp_folders.upload)
                for file_name in files:
                    if re.match(r"^.*\.(onx|xml)$", file_name):
                        try:
                            date_str = re.search(date_regex, file_name).group(0)
                        except AttributeError:
                            msg = f"Could not find date with pattern `{date_regex}` in file name {file_name}"
                            logging.error(msg)
                            raise AirflowException(msg)
                        release_info.append({"release_date": date_str, "file_name": file_name})

            if not bool(release_info):
                raise AirflowSkipException("No new releases available. Skipping downstream DAG tasks.")

            releases = []
            for record in release_info:
                onix_file_name = record["file_name"]
                releases.append(
                    OnixRelease(
                        dag_id=dag_id,
                        run_id=context["run_id"],
                        snapshot_date=pendulum.parse(record["release_date"]),
                        onix_file_name=onix_file_name,
                    )
                )
            return [r.to_dict() for r in releases]

        @task_group(group_id="process_release")
        def process_release(data: dict):
            @task()
            def move_files_to_in_progress(release: dict, **context):
                """Move ONIX files to SFTP in-progress folder.
                :param releases: a list of Onix release instances"""

                release = OnixRelease.from_dict(release)
                sftp_folders.move_files_to_in_progress(release.onix_file_name)

            @task()
            def download(release: dict, **context):
                """Task to download the ONIX releases."""

                release = OnixRelease.from_dict(release)
                with make_sftp_connection(sftp_service_conn_id) as sftp:
                    in_progress_file = os.path.join(sftp_folders.in_progress, release.onix_file_name)
                    sftp.get(in_progress_file, localpath=release.download_path)
                    success = gcs_upload_files(
                        bucket_name=cloud_workspace.download_bucket, file_paths=[release.download_path]
                    )
                    set_task_state(success, context["ti"].task_id, release=release)

            @task()
            def transform(release: dict, **context) -> None:
                """Task to transform the ONIX releases."""

                release = OnixRelease.from_dict(release)

                # Download files from GCS
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_blob_name,
                    file_path=release.download_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file {release.download_blob_name}")

                transformer = OnixTransformer(
                    input_path=release.download_path,
                    output_dir=release.transform_folder,
                    elevate_related_products=elevate_related_products,
                    collapse_subjects=True,
                    save_format="jsonl.gz",
                )
                md_file = transformer.transform()
                shutil.move(md_file, release.transform_path)
                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_path]
                )
                set_task_state(success, context["ti"].task_id, release=release)

            @task()
            def bq_load(release: dict, **context) -> None:
                """Task to load each transformed release to BigQuery."""

                release = OnixRelease.from_dict(release)
                bq_create_dataset(
                    project_id=cloud_workspace.project_id,
                    dataset_id=metadata_partner.bq_dataset_id,
                    location=cloud_workspace.data_location,
                    description=bq_dataset_description,
                )
                client = Client(project=cloud_workspace.project_id)
                # Load each transformed release
                table_id = bq_sharded_table_id(
                    cloud_workspace.project_id,
                    metadata_partner.bq_dataset_id,
                    metadata_partner.bq_table_name,
                    release.snapshot_date,
                )
                uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
                state = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=metadata_partner.schema_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    table_description=bq_table_description,
                    client=client,
                    ignore_unknown_values=True,
                )
                set_task_state(state, context["ti"].task_id, release=release)

            @task()
            def move_files_to_finished(release: dict, **context) -> None:
                """Move ONIX files to SFTP finished folder."""

                release = OnixRelease.from_dict(release)
                sftp_folders.move_files_to_finished(release.onix_file_name)

            @task()
            def add_new_dataset_releases(release: dict, **context) -> None:
                """Adds release information to API."""

                release = OnixRelease.from_dict(release)
                client = Client(project=cloud_workspace.project_id)
                api = DatasetAPI(
                    bq_project_id=cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id, client=client
                )
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    entity_id="onix",
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

                release = OnixRelease.from_dict(release)
                cleanup(dag_id=dag_id, workflow_folder=release.workflow_folder)

            (
                move_files_to_in_progress(data)
                >> download(data)
                >> transform(data)
                >> bq_load(data)
                >> move_files_to_finished(data)
                >> add_new_dataset_releases(data)
                >> cleanup_workflow(data)
            )

        task_check_dependencies = check_dependencies(airflow_conns=[sftp_service_conn_id])
        xcom_releases = fetch_releases()
        process_release_task_group = process_release.expand(data=xcom_releases)

        task_check_dependencies >> xcom_releases >> process_release_task_group

    return onix_telescope()
