# Copyright 2023 Curtin University
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
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat

from oaebu_workflows.onix_utils import (
    onix_parser_download,
    onix_parser_execute,
    collapse_subjects,
    create_personname_fields,
)
from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.bigquery import bq_load_table, bq_sharded_table_id, bq_create_dataset
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.url_utils import retry_get_url
from observatory.platform.files import save_jsonl_gz, load_jsonl
from observatory.platform.gcs import gcs_upload_files, gcs_blob_name_from_path, gcs_blob_uri
from observatory.platform.workflows.workflow import (
    Workflow,
    SnapshotRelease,
    make_snapshot_date,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


THOTH_URL = "{host_name}/specifications/{format_specification}/publisher/{publisher_id}"
DEFAULT_HOST_NAME = "https://export.thoth.pub"


class ThothRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
    ):
        """Construct a ThothRelease.
        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param release_date: The date of the snapshot_date/release
        """
        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.download_path = os.path.join(self.download_folder, f"thoth_{snapshot_date.format('YYYY_MM_DD')}.xml")
        self.transform_path = os.path.join(self.download_folder, f"thoth_{snapshot_date.format('YYYY_MM_DD')}.jsonl.gz")


class ThothTelescope(Workflow):
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        publisher_id: str,
        format_specification: str,
        metadata_partner: Union[str, OaebuPartner] = "thoth",
        bq_dataset_description: str = "Thoth ONIX Feed",
        bq_table_description: str = None,
        api_dataset_id: str = "onix",
        host_name: str = "https://export.thoth.pub",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        catchup: bool = False,
        start_date: pendulum.DateTime = pendulum.datetime(2022, 12, 1),
        schedule: str = "@weekly",
    ):
        """Construct an ThothOnixTelescope instance.
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param publisher_id: The Thoth ID for this publisher
        :param format_specification: The Thoth ONIX/metadata format specification. e.g. "onix_3.0::oapen"
        :param metadata_partner: The metadata partner name
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param host_name: The Thoth host name
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param catchup: Whether to catchup the DAG or not
        :param start_date: The start date of the DAG
        :param schedule: The schedule interval of the DAG
        """
        super().__init__(
            dag_id,
            start_date=start_date,
            schedule=schedule,
            airflow_conns=[observatory_api_conn_id],
            catchup=catchup,
            tags=["oaebu"],
        )

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.publisher_id = publisher_id
        self.metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.host_name = host_name
        self.format_specification = format_specification
        self.observatory_api_conn_id = observatory_api_conn_id

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> ThothRelease:
        """Creates a new Thoth release instance

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: The Thoth release instance
        """
        snapshot_date = make_snapshot_date(**kwargs)
        release = ThothRelease(dag_id=self.dag_id, run_id=kwargs["run_id"], snapshot_date=snapshot_date)
        return release

    def download(self, release: ThothRelease, **kwargs) -> None:
        """Task to download the ONIX release from Thoth.

        :param release: The Thoth release instance
        """
        thoth_download_onix(
            publisher_id=self.publisher_id,
            format_spec=self.format_specification,
            download_path=release.download_path,
        )

    def upload_downloaded(self, release: ThothRelease, **kwargs) -> None:
        """Upload the downloaded thoth onix XML to google cloud bucket"""
        success = gcs_upload_files(bucket_name=self.cloud_workspace.download_bucket, file_paths=[release.download_path])
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, release: ThothRelease, **kwargs) -> None:
        """Task to transform the Thoth ONIX data"""
        success, parser_path = onix_parser_download()
        set_task_state(success, task_id=kwargs["ti"].task_id, release=release)
        success = onix_parser_execute(
            parser_path, input_dir=release.download_folder, output_dir=release.transform_folder
        )
        set_task_state(success, task_id=kwargs["ti"].task_id, release=release)
        transformed = collapse_subjects(load_jsonl(os.path.join(release.transform_folder, "full.jsonl")))
        transformed = create_personname_fields(transformed)
        save_jsonl_gz(release.transform_path, transformed)

    def upload_transformed(self, release: ThothRelease, **kwargs) -> None:
        """Upload the downloaded thoth onix .jsonl to google cloud bucket"""
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.transform_path]
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, release: ThothRelease, **kwargs) -> None:
        """Task to load the transformed ONIX jsonl file to BigQuery."""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.metadata_partner.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )
        uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
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

    def add_new_dataset_releases(self, release: ThothRelease, **kwargs) -> None:
        """Adds release information to API."""
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: ThothRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


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
