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

# Author: Aniek Roelofs

import csv
import os
import re
from collections import OrderedDict, defaultdict
from typing import List, Tuple, Union

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import save_jsonl_gz
from observatory.platform.files import convert, add_partition_date
from observatory.platform.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory.platform.sftp import SftpFolders, make_sftp_connection
from observatory.platform.workflows.workflow import (
    PartitionRelease,
    Workflow,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


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
        self.download_sales_path = os.path.join(self.download_folder, "google_books_sales.csv")
        self.download_traffic_path = os.path.join(self.download_folder, "google_books_traffic.csv")
        self.transform_sales_path = os.path.join(self.transform_folder, "google_books_sales.jsonl.gz")
        self.transform_traffic_path = os.path.join(self.transform_folder, "google_books_traffic.jsonl.gz")
        self.sftp_files = sftp_files


class GoogleBooksTelescope(Workflow):
    """The Google Books telescope."""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        sftp_root: str = "/",
        sales_partner: Union[str, OaebuPartner] = "google_books_sales",
        traffic_partner: Union[str, OaebuPartner] = "google_books_traffic",
        bq_dataset_description: str = "Data from Google sources",
        bq_sales_table_description: str = None,
        bq_traffic_table_description: str = None,
        api_dataset_id: str = "google_books",
        sftp_service_conn_id: str = "sftp_service",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        catchup: bool = False,
        schedule: str = "@weekly",
        start_date: pendulum.DateTime = pendulum.datetime(2018, 1, 1),
    ):
        """Construct a GoogleBooksTelescope instance.
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param sftp_root: The root of the SFTP filesystem to work with
        :param sales_partner: The name of the sales partner
        :param traffic_partner: The name of the traffic partner
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_sales_table_description: Description for the BigQuery Google Books Sales table
        :param bq_traffic_table_description: Description for the BigQuery Google Books Traffic table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param sftp_service_conn_id: Airflow connection ID for the SFTP service
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param catchup: Whether to catchup the DAG or not
        :param schedule: The schedule interval of the DAG
        :param start_date: The start date of the DAG
        """
        super().__init__(
            dag_id,
            start_date,
            schedule,
            catchup=catchup,
            airflow_conns=[sftp_service_conn_id, observatory_api_conn_id],
            tags=["oaebu"],
        )
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.sftp_root = sftp_root
        self.sales_partner = partner_from_str(sales_partner)
        self.traffic_partner = partner_from_str(traffic_partner)
        self.bq_dataset_description = bq_dataset_description
        self.bq_sales_table_description = bq_sales_table_description
        self.bq_traffic_table_description = bq_traffic_table_description
        self.api_dataset_id = api_dataset_id
        self.sftp_service_conn_id = sftp_service_conn_id
        self.observatory_api_conn_id = observatory_api_conn_id

        # Extra SFTP parameters
        self.sftp_folders = SftpFolders(dag_id, sftp_conn_id=sftp_service_conn_id, sftp_root=sftp_root)
        self.sftp_regex = r"^Google(SalesTransaction|BooksTraffic)Report_\d{4}_\d{2}.csv$"

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

    def make_release(self, **kwargs) -> List[GoogleBooksRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: A list of google books release instances
        """
        ti: TaskInstance = kwargs["ti"]
        reports_info = ti.xcom_pull(
            key=GoogleBooksTelescope.RELEASE_INFO, task_ids=self.list_release_info.__name__, include_prior_dates=False
        )
        releases = []
        run_id = kwargs["run_id"]
        for partition_date, sftp_files in reports_info.items():
            releases.append(
                GoogleBooksRelease(
                    self.dag_id, run_id=run_id, partition_date=pendulum.parse(partition_date), sftp_files=sftp_files
                )
            )
        return releases

    def list_release_info(self, **kwargs) -> bool:
        """Lists all Google Books releases available on the SFTP server and publishes sftp file paths and
        release_date's as an XCom.

        :return: the identifier of the task to execute next.
        """

        reports = defaultdict(list)
        # List all reports in the 'upload' folder of the organisation
        with make_sftp_connection(self.sftp_service_conn_id) as sftp:
            files = sftp.listdir(self.sftp_folders.upload)
            for file_name in files:
                match = re.match(self.sftp_regex, file_name)
                if match:
                    # Get the release date from file name
                    date_str = file_name[-11:].strip(".csv")
                    release_date = pendulum.from_format(date_str, "YYYY_MM").end_of("month")
                    release_date = release_date.format("YYYYMMDD")

                    # Get the report type from file name
                    report_type = match.group(1)

                    # Create the full path of the file for the 'in progress' folder
                    sftp_file = os.path.join(self.sftp_folders.in_progress, file_name)

                    # Append report
                    reports[report_type + release_date].append(sftp_file)

        # Check that for each report type + date combination there is a report available
        release_info = defaultdict(list)
        for report, sftp_files in reports.items():
            release_date = report[-8:]
            release_info[release_date] += sftp_files

        continue_dag = bool(release_info)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(GoogleBooksTelescope.RELEASE_INFO, release_info)

        return continue_dag

    def move_files_to_in_progress(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Move Google Books files to SFTP in-progress folder."""

        for release in releases:
            self.sftp_folders.move_files_to_in_progress(release.sftp_files)

    def download(self, releases: List[GoogleBooksRelease], **kwargs):
        """Task to download the Google Books releases for a given month."""
        for release in releases:
            with make_sftp_connection(self.sftp_service_conn_id) as sftp:
                for file in release.sftp_files:
                    if "Traffic" in file:
                        sftp.get(file, localpath=release.download_traffic_path)
                    elif "Transaction" in file:
                        sftp.get(file, localpath=release.download_sales_path)
            assert os.path.exists(release.download_traffic_path) and os.path.exists(release.download_sales_path)

    def upload_downloaded(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Uploads the downloaded files to GCS for each release"""
        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.download_bucket,
                file_paths=[release.download_sales_path, release.download_traffic_path],
            )
            if not success:
                raise AirflowException(f"Files could not be uploaded to cloud storage bucket: {self.transform_bucket}")

    def transform(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Task to transform the Google Books releases for a given month."""
        for release in releases:
            gb_transform(
                download_files=(release.download_sales_path, release.download_traffic_path),
                sales_path=release.transform_sales_path,
                traffic_path=release.transform_traffic_path,
                release_date=release.partition_date,
            )

    def upload_transformed(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Uploads the transformed files to GCS for each release"""
        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.transform_bucket,
                file_paths=[release.transform_sales_path, release.transform_traffic_path],
            )
            if not success:
                raise AirflowException(f"Files could not be uploaded to cloud storage bucket: {self.transform_bucket}")

    def move_files_to_finished(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Move Google Books files to SFTP finished folder."""

        for release in releases:
            self.sftp_folders.move_files_to_finished(release.sftp_files)

    def bq_load(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Loads the sales and traffic data into BigQuery"""
        for release in releases:
            for partner, table_description, file_path in [
                [self.sales_partner, self.bq_sales_table_description, release.transform_sales_path],
                [self.traffic_partner, self.bq_traffic_table_description, release.transform_traffic_path],
            ]:
                bq_create_dataset(
                    project_id=self.cloud_workspace.project_id,
                    dataset_id=partner.bq_dataset_id,
                    location=self.cloud_workspace.data_location,
                    description=self.bq_dataset_description,
                )
                uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(file_path))
                table_id = bq_table_id(self.cloud_workspace.project_id, partner.bq_dataset_id, partner.bq_table_name)
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
                )
                set_task_state(success, kwargs["ti"].task_id, release=release)

    def add_new_dataset_releases(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Adds release information to API."""

        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        for release in releases:
            dataset_release = DatasetRelease(
                dag_id=self.dag_id,
                dataset_id=self.api_dataset_id,
                dag_run_id=release.run_id,
                data_interval_start=kwargs["data_interval_start"],
                data_interval_end=kwargs["data_interval_end"],
                partition_date=release.partition_date,
            )
            api.post_dataset_release(dataset_release)

    def cleanup(self, releases: List[GoogleBooksRelease], **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        for release in releases:
            cleanup(
                dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder
            )


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
