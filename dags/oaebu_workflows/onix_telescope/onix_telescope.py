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
from typing import List, Union

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowSkipException
from google.cloud.bigquery import SourceFormat, Client

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from oaebu_workflows.onix_utils import collapse_subjects, onix_parser_download, onix_parser_execute
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import load_jsonl, save_jsonl_gz
from observatory_platform.google.gcs import gcs_blob_uri, gcs_upload_files, gcs_blob_name_from_path, gcs_download_blob
from observatory_platform.google.bigquery import bq_load_table, bq_sharded_table_id, bq_create_dataset
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.sftp import SftpFolders, make_sftp_connection
from observatory_platform.airflow.release import SnapshotRelease, set_task_state
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup


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
    bq_dataset_description: str = "ONIX data provided by Org",
    bq_table_description: str = None,
    api_dataset_id: str = "onix",
    sftp_service_conn_id: str = "sftp_service",
    catchup: bool = False,
    schedule: str = "@weekly",
    start_date: pendulum.DateTime = pendulum.datetime(2021, 3, 28),
):
    """Construct an OINX DAG.
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param sftp_root: The working root of the SFTP server, passed to the SftoFolders class
    :param metadata_partner: The metadata partner name
    :param date_regex: Regular expression for extracting a date string from an ONIX file name
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_dataset_id: The ID to store the dataset release in the API
    :param sftp_service_conn_id: Airflow connection ID for the SFTP service
    :param catchup: Whether to catchup the DAG or not
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    """
    metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
    sftp_folders = SftpFolders(dag_id, sftp_conn_id=sftp_service_conn_id, sftp_root=sftp_root)

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        tags=["oaebu"],
        default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=5)},
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
            def move_files_to_in_progress(release: dict, **context) -> None:
                """Move ONIX files to SFTP in-progress folder.
                :param releases: a list of Onix release instances"""

                release = OnixRelease(release)
                sftp_folders.move_filesrto_in_progress(release.onix_file_name)

            @task()
            def download(release: dict, **context):
                """Task to download the ONIX releases."""

                release = OnixRelease(release)
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

                release = OnixRelease(release)

                # Download the parser
                success, parser_path = onix_parser_download()
                set_task_state(success, context["ti"].task_id)

                # Download files from GCS
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_blob_name,
                    file_path=release.download_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file {release.download_blob_name}")

                onix_parser_execute(
                    parser_path=parser_path, input_dir=release.download_folder, output_dir=release.transform_folder
                )
                onix = collapse_subjects(load_jsonl(release.parsed_path))
                save_jsonl_gz(release.transform_path, onix)
                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_path]
                )
                set_task_state(success, context["ti"].task_id, release=release)

            @task()
            def bq_load(release: dict, **context) -> None:
                """Task to load each transformed release to BigQuery."""

                release = OnixRelease(release)
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
                )
                set_task_state(state, context["ti"].task_id, release=release)

            @task()
            def move_files_to_finished(release: dict, **context) -> None:
                """Move ONIX files to SFTP finished folder."""

                release = OnixRelease(release)
                sftp_folders.move_files_to_finished(release.onix_file_name)

            @task()
            def add_new_dataset_releases(release: dict, **context) -> None:
                """Adds release information to API."""

                release = OnixRelease(release)
                client = Client(project=cloud_workspace.project_id)
                api = DatasetAPI(project_id=cloud_workspace.project_id, client=client)
                api.seed_db()
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    dataset_id=api_dataset_id,
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

                release = OnixRelease(release)
                cleanup(
                    dag_id=dag_id, execution_date=context["execution_date"], workflow_folder=release.workflow_folder
                )

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
