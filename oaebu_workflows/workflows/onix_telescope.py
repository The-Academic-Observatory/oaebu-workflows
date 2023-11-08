# Copyright 2021-2023 Curtin University
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
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from oaebu_workflows.onix_utils import collapse_subjects, onix_parser_download, onix_parser_execute
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import load_jsonl, save_jsonl_gz
from observatory.platform.gcs import gcs_blob_uri, gcs_upload_files, gcs_blob_name_from_path
from observatory.platform.bigquery import bq_load_table, bq_sharded_table_id, bq_create_dataset
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.sftp import SftpFolders, make_sftp_connection
from observatory.platform.workflows.workflow import (
    SnapshotRelease,
    Workflow,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


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
        self.download_path = os.path.join(self.download_folder, self.onix_file_name)
        self.parsed_path = os.path.join(self.transform_folder, "full.jsonl")
        self.transform_path = os.path.join(self.transform_folder, "onix.jsonl.gz")


class OnixTelescope(Workflow):
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        date_regex: str,
        sftp_root: str = "/",
        metadata_partner: Union[str, OaebuPartner] = "onix",
        bq_dataset_description: str = "ONIX data provided by Org",
        bq_table_description: str = None,
        api_dataset_id: str = "onix",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        sftp_service_conn_id: str = "sftp_service",
        catchup: bool = False,
        schedule: str = "@weekly",
        start_date: pendulum.DateTime = pendulum.datetime(2021, 3, 28),
    ):
        """Construct an OnixTelescope instance.
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param sftp_root: The working root of the SFTP server, passed to the SftoFolders class
        :param metadata_partner: The metadata partner name
        :param date_regex: Regular expression for extracting a date string from an ONIX file name
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param sftp_service_conn_id: Airflow connection ID for the SFTP service
        :param catchup: Whether to catchup the DAG or not
        :param schedule: The schedule interval of the DAG
        :param start_date: The start date of the DAG
        """
        super().__init__(
            dag_id,
            start_date,
            schedule,
            catchup=catchup,
            airflow_conns=[observatory_api_conn_id, sftp_service_conn_id],
            tags=["oaebu"],
        )
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.sftp_root = sftp_root
        self.metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
        self.date_regex = date_regex
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.observatory_api_conn_id = observatory_api_conn_id
        self.sftp_service_conn_id = sftp_service_conn_id
        self.sftp_folders = SftpFolders(dag_id, sftp_conn_id=sftp_service_conn_id, sftp_root=sftp_root)

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.list_release_info)
        self.add_task(self.move_files_to_in_progress)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.move_files_to_finished)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def list_release_info(self, **kwargs):
        """Lists all ONIX releases and publishes their file names as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: the identifier of the task to execute next.
        """

        # List release dates
        release_info = []
        with make_sftp_connection(self.sftp_service_conn_id) as sftp:
            files = sftp.listdir(self.sftp_folders.upload)
            for file_name in files:
                if re.match(r"^.*\.(onx|xml)$", file_name):
                    try:
                        date_str = re.search(self.date_regex, file_name).group(0)
                    except AttributeError:
                        msg = f"Could not find date with pattern `{self.date_regex}` in file name {file_name}"
                        logging.error(msg)
                        raise AirflowException(msg)
                    release_info.append({"release_date": date_str, "file_name": file_name})

        # Publish XCom
        continue_dag = len(release_info)
        if continue_dag:
            ti: TaskInstance = kwargs["ti"]
            execution_date = kwargs["execution_date"]
            ti.xcom_push(OnixTelescope.RELEASE_INFO, release_info, execution_date)

        return continue_dag

    def make_release(self, **kwargs) -> List[OnixRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :return: a list of Onix release instances.
        """

        ti: TaskInstance = kwargs["ti"]
        records = ti.xcom_pull(
            key=OnixTelescope.RELEASE_INFO, task_ids=self.list_release_info.__name__, include_prior_dates=False
        )
        releases = []
        for record in records:
            onix_file_name = record["file_name"]
            releases.append(
                OnixRelease(
                    dag_id=self.dag_id,
                    run_id=kwargs["run_id"],
                    snapshot_date=pendulum.parse(record["release_date"]),
                    onix_file_name=onix_file_name,
                )
            )
        return releases

    def move_files_to_in_progress(self, releases: List[OnixRelease], **kwargs):
        """Move ONIX files to SFTP in-progress folder.
        :param releases: a list of Onix release instances"""
        self.sftp_folders.move_files_to_in_progress([release.onix_file_name for release in releases])

    def download(self, releases: List[OnixRelease], **kwargs):
        """Task to download the ONIX releases."""
        with make_sftp_connection(self.sftp_service_conn_id) as sftp:
            for release in releases:
                in_progress_file = os.path.join(self.sftp_folders.in_progress, release.onix_file_name)
                sftp.get(in_progress_file, localpath=release.download_path)

    def upload_downloaded(self, releases: List[OnixRelease], **kwargs):
        """Uploads the downloaded onix file to GCS"""
        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.download_bucket, file_paths=[release.download_path]
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, releases: List[OnixRelease], **kwargs):
        """Task to transform the ONIX releases."""

        success, parser_path = onix_parser_download()
        set_task_state(success, kwargs["ti"].task_id)
        for release in releases:
            onix_parser_execute(
                parser_path=parser_path, input_dir=release.download_folder, output_dir=release.transform_folder
            )
            onix = collapse_subjects(load_jsonl(release.parsed_path))
            save_jsonl_gz(release.transform_path, onix)

    def upload_transformed(self, releases: List[OnixRelease], **kwargs):
        """Uploads the transformed file to GCS"""
        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, releases: List[OnixRelease], **kwargs):
        """Task to load each transformed release to BigQuery."""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.metadata_partner.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )
        # Load each transformed release
        for release in releases:
            table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.metadata_partner.bq_dataset_id,
                self.metadata_partner.bq_table_name,
                release.snapshot_date,
            )
            uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
            state = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=self.metadata_partner.schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                table_description=self.bq_table_description,
            )
            set_task_state(state, kwargs["ti"].task_id, release=release)

    def move_files_to_finished(self, releases: List[OnixRelease], **kwargs):
        """Move ONIX files to SFTP finished folder."""
        self.sftp_folders.move_files_to_finished([release.onix_file_name for release in releases])

    def add_new_dataset_releases(self, releases: List[OnixRelease], **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        for release in releases:
            dataset_release = DatasetRelease(
                dag_id=self.dag_id,
                dataset_id=self.api_dataset_id,
                dag_run_id=release.run_id,
                snapshot_date=release.snapshot_date,
                data_interval_start=kwargs["data_interval_start"],
                data_interval_end=kwargs["data_interval_end"],
            )
            api.post_dataset_release(dataset_release)

    def cleanup(self, releases: List[OnixRelease], **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        for release in releases:
            cleanup(
                dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder
            )
