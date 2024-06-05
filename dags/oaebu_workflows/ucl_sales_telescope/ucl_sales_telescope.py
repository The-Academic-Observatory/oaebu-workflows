# Copyright 2024 Curtin University
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


import collections import defaultdict
import logging
import os
from typing import List, Union

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from google.cloud.bigquery import SourceFormat, TimePartitioningType, WriteDisposition, Client
from google.oauth2 import service_account
from apiclient import discovery

from oaebu_workflows.config import sql_folder
from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import save_jsonl_gz, load_jsonl
from observatory_platform.google.gcs import gcs_blob_uri, gcs_upload_files, gcs_blob_name_from_path, gcs_download_blob
from observatory_platform.google.bigquery import bq_load_table, bq_table_id, bq_create_dataset, bq_run_query
from observatory_platform.url_utils import retry_get_url
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.files import add_partition_date
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.airflow import on_failure_callback


class UclSalesRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
    ):
        """Construct a UclSalesRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID.
        :param data_interval_start: The start of the data interval.
        :param data_interval_end: The end of the data interval.
        :param partition_date: The partition date for this release.
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.transform_file_name = "ucl_sales.jsonl.gz"

    @property
    def download_path(self):
        return os.path.join(self.download_folder, "ucl_sales.jsonl.gz")

    @property
    def transform_path(self):
        return os.path.join(self.transform_folder, "ucl_sales.jsonl.gz")

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.downloady_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return UclSalesRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            data_interval_start=pendulum.from_format(dict_["data_interval_start"], "YYYY-MM-DD"),
            data_interval_end=pendulum.from_format(dict_["data_interval_end"], "YYYY-MM-DD"),
            partition_date=pendulum.from_format(dict_["partition_date"], "YYYY-MM-DD"),
        )

    def to_dict(self) -> dict:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "data_interval_start": self.data_interval_start.to_date_string(),
            "data_interval_end": self.data_interval_end.to_date_string(),
            "partition_date": self.partition_date.to_date_string(),
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    sheet_id: str,
    data_partner: Union[str, OaebuPartner] = "ucl_sales",
    bq_dataset_description: str = "UCL Sales Dataset",
    bq_table_description: str = "UCL Sales Table",
    api_bq_dataset_id: str = "dataset_api",
    oaebu_service_account_conn_id: str = "oaebu_service_account",
    max_threads: int = os.cpu_count() * 2,
    schedule: str = "0 0 4 * *",  # run on the 4th of every month TODO: confirm
    start_date: pendulum.DateTime = pendulum.datetime(2023, 8, 1),
    catchup: bool = True,
    max_active_runs: int = 10,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """Construct a UclSales DAG.

    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param sheet_id:  The ID of the google sheet containing the sales data
    :param data_partner: The name of the data partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_bq_dataset_id: The name of the Bigquery dataset to store the API release(s)
    :param oaebu_service_account_conn_id: Airflow connection ID for the oaebu service account
    :param max_threads: The maximum number threads to utilise for parallel processes
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    :param catchup: Whether to catchup the DAG or not
    :param max_active_runs: The maximum number of concurrent DAG runs
    :param retries: The number of times to retry failed tasks
    :param retry_delay: The delay between retries in minutes
    """
    data_partner = partner_from_str(data_partner)

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
    def ucl_sales():
        @task()
        def _make_release(**context) -> dict:
            return make_release(dag_id, context).to_dict()

        @task()
        def _download(release: dict, **context) -> None:
            """Download the ucl sales data for a given release.
            :param releases: The UCL discovery release.
            """
            scopes = [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/spreadsheets",
            ]
            service_account_conn = BaseHook.get_connection(oaebu_service_account_conn_id)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_conn.extra_dejson, scopes=scopes
            )
            sheet_data = download(credentials, sheet_id)

            logging.info(f"Saving downloaded data to file: {release.download_path}")
            save_jsonl_gz(release.download_path, sheet_data)

            success = gcs_upload_files(bucket_name=cloud_workspace.download_bucket, file_paths=[release.download_path])
            set_task_state(success, context["ti"].task_id, release=release)

        @task()
        def _transform(release: dict, **context) -> None:
            """Transform the ucl discovery data for a given release."""

            release = UclSalesRelease.from_dict(release)
            # Download files from GCS
            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.download_blob_name,
                file_path=release.download_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_blob_name}")

            client = Client(project=cloud_workspace.project_id)
            book_list = get_ucl_book_list(client=client)
            data = load_jsonl(release.download_path)
            data = transform(data, book_list)

            save_jsonl_gz(release.transform_path, data)
            success = gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task()
        def _bq_load(release: dict, **context) -> None:
            """Loads the transformed data into BigQuery"""

            release = UclSalesRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=data_partner.bq_dataset_id,
                location=cloud_workspace.data_location,
                description=bq_dataset_description,
            )

            uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
            table_id = bq_table_id(cloud_workspace.project_id, data_partner.bq_dataset_id, data_partner.bq_table_name)
            client = Client(project=cloud_workspace.project_id)
            state = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=data_partner.schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition_type=TimePartitioningType.MONTH,
                partition=True,
                partition_field="release_date",
                write_disposition=WriteDisposition.WRITE_APPEND,
                table_description=bq_table_description,
                ignore_unknown_values=True,
                client=client,
            )
            set_task_state(state, context["ti"].task_id, release=release)

        @task()
        def _add_new_dataset_releases(release: dict, **context) -> None:
            """Adds release information to API."""

            release = UclSalesRelease.from_dict(release)
            client = Client(project=cloud_workspace.project_id)
            api = DatasetAPI(bq_project_id=cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id, client=client)
            api.seed_db()
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                entity_id="ucl_sales",
                dag_run_id=release.run_id,
                created=pendulum.now(),
                modified=pendulum.now(),
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
                partition_date=release.partition_date,
            )
            api.add_dataset_release(dataset_release)

        @task()
        def _cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            release = UclSalesRelease.from_dict(release)
            cleanup(dag_id=dag_id, execution_date=context["execution_date"], workflow_folder=release.workflow_folder)

        task_check_dependencies = check_dependencies(airflow_conns=[oaebu_service_account_conn_id])
        xcom_release = _make_release()
        task_download = _download(xcom_release)
        task_transform = _transform(xcom_release)
        task_bq_load = _bq_load(xcom_release)
        task_add_new_dataset_releases = _add_new_dataset_releases(xcom_release)
        task_cleanup_workflow = _cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> xcom_release
            >> task_download
            >> task_transform
            >> task_bq_load
            >> task_add_new_dataset_releases
            >> task_cleanup_workflow
        )

    return ucl_sales()


def make_release(dag_id: str, context: dict) -> UclSalesRelease:
    """Creates a new ucl discovery release instance

    :param dag_id: The ID of the dag to make the release for.
    :param context: the context passed from the PythonOperator.
    See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
    :return: A UclSales release instance
    """

    data_interval_start = context["data_interval_start"].start_of("month")
    data_interval_end = context["data_interval_end"].start_of("month")
    partition_date = data_interval_start.end_of("month")
    run_id = context["run_id"]

    logging.info(
        f"Interval Start: {data_interval_start}, Interval End:{data_interval_end}, Partition date: {partition_date}, Run ID: {run_id}"
    )
    return UclSalesRelease(
        dag_id,
        context["run_id"],
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        partition_date=partition_date,
    )


def download(credentials: service_account.Credentials, sheet_id: str, sheet_month: str) -> List[dict]:
    """Downloads the UCL sales data for a given month (sheet_month) from the google sheet

    :param credentials: The google application credentials for sheet access.
    :param sheet_id: The ID of the google sheet. Can be found in its URL.
    :param sheet_month: The month to download. In the form YYYYMM.
    :return: The downloaded data as a list of dictionaries
    """

    service = discovery.build("sheets", "v4", credentials=credentials)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_month).execute()
    sheet_contents = result.get("values")
    if not sheet_contents:
        raise ValueError(f"No content found for sheet with ID '{sheet_id}' and month '{sheet_month}'")

    # In case of inconsistencies, reformat the header
    header = [c.strip().upper() for c in sheet_contents[0]]
    if not all(heading in header for heading in ["ISBN", "QTY", "YEAR", "MONTH", "FREE/PAID/RETURN?"]):
        raise ValueError(f"Invalid header found for sheet: {header}")

    items = []
    for row in sheet_contents[1:]:
        items.append(dict(zip(header, row)))
    return items


def get_ucl_book_list(client: Client = None) -> List[dict]:
    """Retrieves the ucl book list from the data export dataset

    :param client: The bigquery client to use
    :return: The resulting rows from the query
    """

    sql_file = os.path.join(sql_folder("ucl_sales_telescope"), "book_list_query.sql")
    with open(sql_file, "r") as f:
        sql = f.read()
    return bq_run_query(sql, client=client)


def transform(data: List[dict], book_list: List[dict]) -> List[dict]:
    """Transforms the ucl sales data. Aggregates each sale and matches the product using the book list

    :param data: The UCL sales data
    :param book_list: The UCL book list from its export table
    :return: The transformed data
    """

    isbn_net_qty= defaultdict(lambda: 0)
    for entry in data:
        if entry["FREE/PAID/RETURN?"] == "Paid":
            isbn_net_qty[entry["ISBN"]] += entry["QTY"]
        elif entry["FREE/PAID/RETURN?"] == "Return":
            isbn_net_qty[entry["ISBN"]] -= entry["QTY"]
    # TOOD: figure out the rest



        
