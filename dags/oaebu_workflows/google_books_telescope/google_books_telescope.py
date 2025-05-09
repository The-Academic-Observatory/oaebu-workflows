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

import csv
import os
import re
from collections import OrderedDict, defaultdict
from typing import List, Tuple, Union

import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.decorators import dag, task, task_group
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition, Client
from google.cloud.bigquery.table import Row

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import convert, add_partition_date, save_jsonl_gz
from observatory_platform.google.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path, gcs_download_blob
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.google.bigquery import bq_load_table, bq_table_id, bq_create_dataset, bq_run_query
from observatory_platform.sftp import SftpFolders, make_sftp_connection
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.airflow import on_failure_callback


class GoogleBooksRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        partition_date: pendulum.DateTime,
        sftp_files: List[str],
    ):
        """Construct a GoogleBooksRelease.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param partition_date: the partition date, corresponds to the last day of the month being processed.
        :param sftp_files: List of full filepaths to download from sftp service (incl. in_progress folder)
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.download_sales_file_name = "google_books_sales.csv"
        self.download_traffic_file_name = "google_books_traffic.csv"
        self.transform_sales_file_name = "google_books_sales.jsonl.gz"
        self.transform_traffic_file_name = "google_books_traffic.jsonl.gz"
        self.sftp_files = sftp_files

    @property
    def download_sales_path(self):
        return os.path.join(self.download_folder, self.download_sales_file_name)

    @property
    def download_traffic_path(self):
        return os.path.join(self.download_folder, self.download_traffic_file_name)

    @property
    def transform_sales_path(self):
        return os.path.join(self.transform_folder, self.transform_sales_file_name)

    @property
    def transform_traffic_path(self):
        return os.path.join(self.transform_folder, self.transform_traffic_file_name)

    @property
    def download_sales_blob_name(self):
        return gcs_blob_name_from_path(self.download_sales_path)

    @property
    def download_traffic_blob_name(self):
        return gcs_blob_name_from_path(self.download_traffic_path)

    @property
    def transform_sales_blob_name(self):
        return gcs_blob_name_from_path(self.transform_sales_path)

    @property
    def transform_traffic_blob_name(self):
        return gcs_blob_name_from_path(self.transform_traffic_path)

    @staticmethod
    def from_dict(dict_: dict):
        return GoogleBooksRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            partition_date=pendulum.parse(dict_["partition_date"]),
            sftp_files=dict_["sftp_files"],
        )

    def to_dict(self):
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "partition_date": self.partition_date.to_date_string(),
            "sftp_files": self.sftp_files,
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    sftp_root: str = "/",
    sales_partner: Union[str, OaebuPartner] = "google_books_sales",
    traffic_partner: Union[str, OaebuPartner] = "google_books_traffic",
    bq_dataset_description: str = "Data from Google sources",
    bq_sales_table_description: str = None,
    bq_traffic_table_description: str = None,
    api_bq_dataset_id: str = "dataset_api",
    sftp_service_conn_id: str = "sftp_service",
    catchup: bool = False,
    schedule: str = "0 12 * * *",  # Midday everyday
    start_date: pendulum.DateTime = pendulum.datetime(2018, 1, 1),
    max_active_runs: int = 1,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """Construct a GoogleBooks DAG.
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param sftp_root: The root of the SFTP filesystem to work with
    :param sales_partner: The name of the sales partner
    :param traffic_partner: The name of the traffic partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_sales_table_description: Description for the BigQuery Google Books Sales table
    :param bq_traffic_table_description: Description for the BigQuery Google Books Traffic table
    :param api_bq_dataset_id: The name of the Bigquery dataset to store the API release(s)
    :param sftp_service_conn_id: Airflow connection ID for the SFTP service
    :param catchup: Whether to catchup the DAG or not
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    :param max_active_runs: The maximum number of active DAG runs
    :param retries: The number of times to retry failed tasks
    :param retry_delay: The delay between retries in minutes
    """
    sales_partner = partner_from_str(sales_partner)
    traffic_partner = partner_from_str(traffic_partner)

    # Extra SFTP parameters
    sftp_folders = SftpFolders(dag_id, sftp_conn_id=sftp_service_conn_id, sftp_root=sftp_root)
    sftp_regex = r"^Google(SalesTransaction|BooksTraffic)Report_\d{4}_\d{2}.csv$"

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
    def google_books():
        @task
        def fetch_releases(**context) -> List[dict]:
            """Lists all Google Books releases available on the SFTP server

            :returns: List of release dictionaries
            """

            reports = defaultdict(list)
            # List all reports in the 'upload' folder of the organisation
            with make_sftp_connection(sftp_service_conn_id) as sftp:
                files = sftp.listdir(sftp_folders.upload)
                for file_name in files:
                    match = re.match(sftp_regex, file_name)
                    if match:
                        # Get the release date from file name
                        date_str = file_name[-11:].strip(".csv")
                        release_date = pendulum.from_format(date_str, "YYYY_MM").end_of("month")
                        release_date = release_date.format("YYYYMMDD")
                        report_type = match.group(1)  # Get the report type from file name
                        # Create the full path of the file for the 'in progress' folder
                        sftp_file = os.path.join(sftp_folders.in_progress, file_name)
                        reports[report_type + release_date].append(sftp_file)

            # Check that for each report type + date combination there is a report available
            release_info = defaultdict(list)
            for report, sftp_files in reports.items():
                release_date = report[-8:]
                release_info[release_date] += sftp_files

            if not bool(release_info):
                traffic_table_id = bq_table_id(
                    cloud_workspace.project_id, traffic_partner.bq_dataset_id, traffic_partner.bq_table_name
                )
                _gb_early_stop(traffic_table_id, cloud_workspace, logical_date=context["logical_date"])

            releases = []
            run_id = context["run_id"]
            for partition_date, sftp_files in release_info.items():
                releases.append(
                    GoogleBooksRelease(
                        dag_id, run_id=run_id, partition_date=pendulum.parse(partition_date), sftp_files=sftp_files
                    )
                )
            return [r.to_dict() for r in releases]

        @task_group(group_id="process_release")
        def process_release(data: dict, **context):
            """Process the Google Books release."""

            @task
            def move_files_to_in_progress(release: dict, **context) -> None:
                """Move Google Books files to SFTP in-progress folder."""

                release = GoogleBooksRelease.from_dict(release)
                sftp_folders.move_files_to_in_progress(release.sftp_files)

            @task
            def download(release: dict, **context) -> None:
                """Downloads the Google Books release and uploads them to GCS"""

                release = GoogleBooksRelease.from_dict(release)
                with make_sftp_connection(sftp_service_conn_id) as sftp:
                    for file in release.sftp_files:
                        if "Traffic" in file:
                            sftp.get(file, localpath=release.download_traffic_path)
                        elif "Transaction" in file:
                            sftp.get(file, localpath=release.download_sales_path)
                if not os.path.exists(release.download_traffic_path) or not os.path.exists(release.download_sales_path):
                    raise FileNotFoundError(
                        f"Release files not found. {release.download_traffic_path} | {release.download_sales_path}"
                    )

                success = gcs_upload_files(
                    bucket_name=cloud_workspace.download_bucket,
                    file_paths=[release.download_sales_path, release.download_traffic_path],
                )
                if not success:
                    raise AirflowException(
                        f"Files could not be uploaded to cloud storage bucket: {cloud_workspace.download_bucket}"
                    )

            @task
            def transform(release: dict, **context) -> None:
                """Transforms the Google Books release and uploads them to GCS"""

                release = GoogleBooksRelease.from_dict(release)
                # Download files from GCS
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_sales_blob_name,
                    file_path=release.download_sales_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file: {release.download_sales_blob_name}")

                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_traffic_blob_name,
                    file_path=release.download_traffic_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file: {release.download_traffic_blob_name}")

                gb_transform(
                    download_files=(release.download_sales_path, release.download_traffic_path),
                    sales_path=release.transform_sales_path,
                    traffic_path=release.transform_traffic_path,
                    release_date=release.partition_date,
                )
                """Uploads the transformed files to GCS for each release"""
                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket,
                    file_paths=[release.transform_sales_path, release.transform_traffic_path],
                )
                if not success:
                    raise AirflowException(
                        f"Files could not be uploaded to cloud storage bucket: {cloud_workspace.transform_bucket}"
                    )

            @task
            def move_files_to_finished(release: dict, **context) -> None:
                """Move Google Books files to SFTP finished folder."""

                release = GoogleBooksRelease.from_dict(release)
                sftp_folders.move_files_to_finished(release.sftp_files)

            @task
            def bq_load(release: dict, **context) -> None:
                """Loads the sales and traffic data into BigQuery"""

                release = GoogleBooksRelease.from_dict(release)
                client = Client(project=cloud_workspace.project_id)
                for partner, table_description, file_path in [
                    [sales_partner, bq_sales_table_description, release.transform_sales_path],
                    [traffic_partner, bq_traffic_table_description, release.transform_traffic_path],
                ]:
                    bq_create_dataset(
                        project_id=cloud_workspace.project_id,
                        dataset_id=partner.bq_dataset_id,
                        location=cloud_workspace.data_location,
                        description=bq_dataset_description,
                    )
                    uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(file_path))
                    table_id = bq_table_id(cloud_workspace.project_id, partner.bq_dataset_id, partner.bq_table_name)
                    success = bq_load_table(
                        uri=uri,
                        table_id=table_id,
                        schema_file_path=partner.schema_path,
                        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                        partition_type=TimePartitioningType.MONTH,
                        partition=True,
                        partition_field="release_date",
                        write_disposition=WriteDisposition.WRITE_APPEND,
                        table_description=table_description,
                        ignore_unknown_values=True,
                        client=client,
                    )
                    set_task_state(success, context["ti"].task_id, release=release)

            @task
            def add_new_dataset_release(release: dict, **context) -> None:
                """Adds release information to API."""

                release = GoogleBooksRelease.from_dict(release)
                client = Client(project=cloud_workspace.project_id)
                api = DatasetAPI(
                    bq_project_id=cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id, client=client
                )
                # Google Books sales
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    entity_id="google_books_sales",
                    dag_run_id=release.run_id,
                    created=pendulum.now(),
                    modified=pendulum.now(),
                    data_interval_start=context["data_interval_start"],
                    data_interval_end=context["data_interval_end"],
                    partition_date=release.partition_date,
                )
                # Google Books traffic
                api.add_dataset_release(dataset_release)
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    entity_id="google_books_traffic",
                    dag_run_id=release.run_id,
                    created=pendulum.now(),
                    modified=pendulum.now(),
                    data_interval_start=context["data_interval_start"],
                    data_interval_end=context["data_interval_end"],
                    partition_date=release.partition_date,
                )
                api.add_dataset_release(dataset_release)

            @task
            def cleanup_workflow(release: dict, **context) -> None:
                """Delete all files, folders and XComs associated with this release."""

                release = GoogleBooksRelease.from_dict(release)
                cleanup(dag_id=dag_id, workflow_folder=release.workflow_folder)

            (
                move_files_to_in_progress(data)
                >> download(data)
                >> transform(data)
                >> move_files_to_finished(data)
                >> bq_load(data)
                >> add_new_dataset_release(data)
                >> cleanup_workflow(data)
            )

        # Define dag tasks
        task_check_dependencies = check_dependencies(airflow_conns=[sftp_service_conn_id])
        xcom_releases = fetch_releases()
        process_release_task_group = process_release.expand(data=xcom_releases)

        task_check_dependencies >> xcom_releases >> process_release_task_group

    return google_books()


def gb_transform(
    download_files: Tuple[str, str], sales_path: str, traffic_path: str, release_date: pendulum.DateTime
) -> None:
    """Transforms sales and traffic reports. For both reports it transforms the csv into a jsonl file and
    replaces spaces in the keys with underscores.

    :param download_files: The Google Books Sales and Traffic files
    :param sales_path: The file path to save the transformed sales data to
    :param traffic_path: The file path to save the transformed traffic data to
    :param release_date: The release date to use as a partitioning date
    """
    # Sort files to get same hash for unit tests

    results = defaultdict(list)
    results["sales"] = []
    results["traffic"] = []
    for file in download_files:
        report_type = "sales" if "sales" in os.path.basename(file).lower() else "traffic"
        with open(file, encoding="utf-16") as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter="\t")
            for row in csv_reader:
                transformed_row = OrderedDict((convert(k.replace("%", "Perc")), v) for k, v in row.items())
                # Sales transaction report
                if report_type == "sales":
                    transaction_date = pendulum.from_format(transformed_row["Transaction_Date"], "MM/DD/YY")

                    # Sanity check that transaction date is in month of release date
                    if release_date.start_of("month") <= transaction_date <= release_date.end_of("month"):
                        pass
                    else:
                        raise AirflowException(
                            "Transaction date does not fall within release month. "
                            f"Transaction date: {transaction_date.strftime('%Y-%m-%d')}, "
                            f"release month: {release_date.strftime('%Y-%m')}"
                        )

                    # Transform to valid date format
                    transformed_row["Transaction_Date"] = transaction_date.strftime("%Y-%m-%d")

                    # Remove percentage sign
                    transformed_row["Publisher_Revenue_Perc"] = transformed_row["Publisher_Revenue_Perc"].strip("%")
                    # This field is not present for some publishers (UCL Press), for ANU Press the field value is
                    # “E-Book”
                    try:
                        transformed_row["Line_of_Business"]
                    except KeyError:
                        transformed_row["Line_of_Business"] = None
                # Traffic report
                else:
                    # Remove percentage sign
                    transformed_row["Buy_Link_CTR"] = transformed_row["Buy_Link_CTR"].strip("%")

                # Append results
                results[report_type].append(transformed_row)

    for report_type, report_results in results.items():
        report_results = add_partition_date(
            report_results,
            partition_date=release_date,
            partition_type=TimePartitioningType.MONTH,
            partition_field="release_date",
        )
        save_path = sales_path if report_type == "sales" else traffic_path
        print(f"SAVING REPORT '{report_type}' to {save_path}")
        save_jsonl_gz(save_path, report_results)


def _gb_early_stop(table_id: str, cloud_workspace: CloudWorkspace, logical_date: pendulum.DateTime) -> None:
    """Decides how to stop. Will normally send a skip exception. However, if it's past the 4th of the month, will
    send an AirlfowException instead with the intention of making an alert through slack.

    :param table_id: The ID of the traffic table
    :param cloud_workspace: The cloud workspace object
    :param logical_date: The logical date of this run
    """

    client = Client(project=cloud_workspace.project_id)
    partition_key = "release_date"
    dates = get_partitions(table_id, partition_key=partition_key, client=client)
    this_run_date = logical_date.subtract(months=1).end_of("month").date()

    if not dates:  # There are no partitions available
        raise AirflowSkipException("No partitions available and no files required for processing. Skipping.")
    most_recent_pd = dates[0].get(partition_key)  # Latest release date

    if most_recent_pd < this_run_date:
        if logical_date.day > 4:
            raise AirflowException("It's past the 4th and there are no files avialable for upload!")
        else:
            raise AirflowSkipException("No files required for processing. Skipping.")


def get_partitions(table_id: str, partition_key: str = "release_date", client: Client = None) -> List[Row]:
    """Queries the table and returns a list of distinct partitions

    :param table_id: The fully qualified table id to query
    :param partition_key: The name of the column that the table is partitioned on
    :return: Query result as a RowIterator - rows are partition dates in descending order
    """
    query = f"SELECT DISTINCT({partition_key}) FROM {table_id} ORDER BY {partition_key} desc"
    return bq_run_query(query, client=client)
