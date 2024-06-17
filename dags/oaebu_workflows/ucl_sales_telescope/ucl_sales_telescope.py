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


import csv
import logging
import os
from typing import List, Union

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from google.cloud.bigquery import SourceFormat, TimePartitioningType, WriteDisposition, Client
from google.oauth2 import service_account
from apiclient import discovery

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import save_jsonl_gz, load_jsonl, add_partition_date, load_csv
from observatory_platform.google.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory_platform.google.gcs import gcs_blob_uri, gcs_upload_files, gcs_blob_name_from_path, gcs_download_blob


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
        self.sheet_month = self.partition_date.format("YYYYMM")

    @property
    def download_path(self):
        return os.path.join(self.download_folder, "ucl_sales.csv")

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

            release = UclSalesRelease.from_dict(release)
            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            service_account_conn = BaseHook.get_connection(oaebu_service_account_conn_id)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_conn.extra_dejson, scopes=scopes
            )
            sheet_data = download(credentials=credentials, sheet_id=sheet_id, sheet_month=release.sheet_month)

            logging.info(f"Saving downloaded data to file: {release.download_path}")
            with open(release.download_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(sheet_data)

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

            data = load_csv(release.download_path)
            data = transform(data)

            save_jsonl_gz(release.transform_path, data)
            success = gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task()
        def _data_integrity_check(release: dict, **context) -> None:
            """Checks that the transformed data is valid"""

            release = UclSalesRelease.from_dict(release)
            # Download files from GCS
            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.transform_blob_name,
                file_path=release.transform_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_blob_name}")

            data = load_jsonl(release.transform_path)
            success = data_integrity_check(data)
            if not success:
                raise RuntimeError("Data integrity check failed. Check logs for information")

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
        task_data_integrity_check = _data_integrity_check(xcom_release)
        task_bq_load = _bq_load(xcom_release)
        task_add_new_dataset_releases = _add_new_dataset_releases(xcom_release)
        task_cleanup_workflow = _cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> xcom_release
            >> task_download
            >> task_transform
            >> task_data_integrity_check
            >> task_bq_load
            >> task_add_new_dataset_releases
            >> task_cleanup_workflow
        )

    return ucl_sales()


def standardise_header(header: List[str]) -> List[str]:
    return [h.strip().lower() for h in header[0]]


def drop_duplicate_headings(data: List[List]) -> List[List]:
    """Finds duplicate headings and drops the column

    :param data: The data. The header is the first item in the list
    """

    duplicates = []
    header = data[0]
    for h in header:
        indexes = [i for i, v in enumerate(data[0]) if v == h]
        duplicates.extend(indexes[1:])
    for i in sorted(duplicates, reverse=True):
        for row in data:
            del row[i]
    return data


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


def download(
    credentials: service_account.Credentials,
    sheet_id: str,
    sheet_month: str,
) -> List[dict]:
    """Downloads the UCL sales data for a given month (sheet_month) from the google sheet
    Executes a preliminary clean by stripping and lowercasing the heading. Compares this to expected_headings

    :param credentials: The google application credentials for sheet access.
    :param sheet_id: The ID of the google sheet. Can be found in its URL.
    :param sheet_month: The month to download. In the form YYYYMM.
    :param expected_headings: A subset of the headings we expect to find.
    :return: The downloaded data as a list of dictionaries (json-like format)
    """

    service = discovery.build("sheets", "v4", credentials=credentials)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_month).execute()
    sheet_contents = result.get("values")
    if not sheet_contents:
        raise ValueError(f"No content found for sheet with ID '{sheet_id}' and month '{sheet_month}'")

    return sheet_contents


def transform(data: List[List]) -> List[dict]:
    """Transforms the ucl sales data.

    :param data: The UCL sales data
    :return: The transformed data
    """

    data[0] = standardise_header(data[0])  # The first row is the header
    data = drop_duplicate_headings(data)

    # Convert to list of dicts format
    converted_data = []
    for row in data[1:]:
        converted_data.append(dict(zip(data[0], row)))

    # Check that all required headings are present
    headings_mapping = {
        "isbn": "ISBN13",
        "qty": "Quantity",
        "year": "Year",
        "month": "Month",
        "free/paid/return?": "Sale_Type",
        "country": "Country",
        "book": "Title",
        "pub date": "Publication_Date",
    }
    for row in converted_data:
        if not all(h in row.keys() for h in headings_mapping.keys()):
            raise ValueError(f"Invalid header found for row: {row.keys()}")

    transformed = []
    for row in converted_data:
        new_row = {v: row[k] for k, v in headings_mapping.items()}
        # Make the release date partition based on each row's year/month
        release_date = pendulum.datetime(year=int(row["Year"]), month=int(row["Month"]), day=1).end_of("month")
        add_partition_date([new_row], partition_date=release_date, partition_field="release_date")
        transformed.append(new_row)

    return transformed


def data_integrity_check(data: List[dict], current_date: pendulum.DateTime) -> bool:
    """Checks that the ucl sales data is valid

    :param data: The data as a list of dictionaries.
    :param current_date: The date to compare the data to. No date in the data should be after this.
    :return: True/False depending on if the check passed/failed.
    """

    def _isbn_check(isbn):
        if not isbn.startswith("978") or len(isbn) != 13:
            return False
        return True

    is_valid = True

    # Empty check
    if len(data) < 1:
        logging.warn("Empty dataset supplied!")
        is_valid = False

    # Date check
    dates = [pendulum.parse(r["release_date"]) for r in data]
    if not all([r <= current_date for r in dates]):
        logging.warn("Found sale month in the future!")
        is_valid = False

    # Sale type check
    sale_types = [r["Sale_Type"] for r in data]
    if not all([st in ("Paid", "Return", "Free") for st in sale_types]):
        logging.warn("Not all sale types one of 'Paid', 'Return', 'Free'")
        is_valid = False

    # ISBN check
    isbns = [r["ISBN13"] for r in data]
    print("HERE")
    print(len(data[0]["ISBN13"]))
    if not all([_isbn_check(i) for i in isbns]):
        logging.warn("Invalid ISBN found in data")
        is_valid = False

    # Quantity check
    qtys = [int(r["Quantity"]) for r in data]
    if not all([q >= 0 for q in qtys]):
        logging.warn("Negative Quantity found in data")
        is_valid = False

    return is_valid
