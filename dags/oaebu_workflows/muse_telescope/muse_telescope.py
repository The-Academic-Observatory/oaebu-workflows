# Copyright 2022-2024 Curtin University
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

import logging
import os
from typing import List, Tuple, Union
import shutil
import gzip
import csv

import chardet
import pendulum
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task, task_group
from google.cloud.bigquery import SourceFormat, WriteDisposition, Client
from google.cloud.bigquery.table import TimePartitioningType
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition, Client
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import save_jsonl_gz, load_jsonl, add_partition_date
from observatory_platform.google.gcs import (
    gcs_blob_name_from_path,
    gcs_upload_file,
    gcs_upload_files,
    gcs_blob_uri,
    gcs_download_blob,
)
from observatory_platform.google.bigquery import bq_load_table, bq_create_dataset, bq_table_id
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.url_utils import retry_get_url

DATE_PARTITION_FIELD = "release_date"


class MuseRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
    ):
        """Create a MuseRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The airflow run ID
        :param data_interval_start: The beginning of the data interval
        :param data_interval_end: The end of the data interval
        :param partition_date: The release/partition date
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        # TODO: Fix the below file name
        self.download_file_name = "_country.json.gz"
        self.transfrom_file_name = "muse.jsonl.gz"

    @property
    def download_path(self):
        return os.path.join(self.download_folder, self.download_file_name)

    @property
    def transform_path(self):
        return os.path.join(self.transform_folder, self.transfrom_file_name)

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.download_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return MuseRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            data_interval_start=pendulum.parse(dict_["data_interval_start"]),
            data_interval_end=pendulum.parse(dict_["data_interval_end"]),
            partition_date=pendulum.parse(dict_["partition_date"]),
        )

    def to_dict(self):
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
    data_partner: Union[str, OaebuPartner] = "muse",
    bq_dataset_description: str = "MUSE dataset",
    bq_table_description: str = "Muse metrics",
    api_bq_dataset_id: str = "dataset_api",
    gmail_api_conn_id: str = "gmail_api",
    catchup: bool = True,
    schedule: str = "0 0 10 * *",  # Run on the 4th of every month
    start_date: pendulum.DateTime = pendulum.datetime(2022, 4, 1),  # Earliest available data
    max_active_runs: int = 1,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """The Muse Telescope
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param data_partner: The name of the data partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_bq_dataset_id: The name of the Bigquery dataset to store the API release(s)
    :param catchup: Whether to catchup the DAG or not
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    :param max_active_runs: The maximum number of active DAG runs
    :param retries: The number of times to retry failed tasks
    :param retry_delay: The delay between retries in minutes
    """

    data_partner = partner_from_str(data_partner)

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["oaebu"],
        on_failure_callback=on_failure_callback,
        default_args=dict(
            retries=retries, retry_delay=pendulum.duration(minutes=retry_delay), on_failure_callback=on_failure_callback
        ),
        max_active_runs=max_active_runs,
    )
    def muse():
        @task
        def list_reports(self) -> List[dict]:  # TODO: fix this for muse
            """List the available releases by going through the messages of a gmail account and looking for a specific pattern.

            If a message has been processed previously it has a specific label, messages with this label will be skipped.
            The message should include a download url. The head of this download url contains the filename, from which the
            release date and publisher can be derived.

            Lists all Jstor releases for a given month and publishes their report_type, download_url and
            release_date's as an XCom.

            :return: A list if dictionaries representing the messages with reports. Each has keys "type", "url" and "id".
            """
            # List messages with specific query
            list_params = {
                "userId": "me",
                "q": f'-label:{JSTOR_PROCESSED_LABEL_NAME} subject:"JSTOR Publisher Report Available" from:no-reply@ithaka.org',
                "labelIds": ["INBOX"],
                "maxResults": 500,
            }
            available_reports = []
            for message_info in self.get_messages(list_params):
                message_id = message_info["id"]
                message = self.service.users().messages().get(userId="me", id=message_id).execute()

                # get download url
                download_url = None
                message_data = base64.urlsafe_b64decode(message["payload"]["body"]["data"])
                for link in BeautifulSoup(message_data, "html.parser", parse_only=SoupStrainer("a")):
                    if link.text == "Download Completed Report":
                        download_url = link["href"]
                        break
                if download_url is None:
                    raise AirflowException(
                        f"Can't find download link for report in e-mail, message snippet: {message.snippet}"
                    )

                # Get filename and extension from headXX
                filename, extension = self.get_header_info(download_url)

                # Get publisher
                report_publisher = filename.split("_")[1]
                if report_publisher != self.entity_id:
                    logging.info(
                        f"Skipping report, because the publisher id in the report's file name '{report_publisher}' "
                        f"does not match the current publisher id: {self.entity_id}"
                    )
                    continue

                # get report_type
                report_mapping = {"PUBBCU": "country", "PUBBIU": "institution"}
                report_type = report_mapping.get(filename.split("_")[2])
                if report_type is None:
                    logging.info(f"Skipping unrecognized report type, filename {filename}")

                # check format
                original_email = f"https://mail.google.com/mail/u/0/#all/{message_id}"
                if extension == "tsv":
                    # add report info
                    logging.info(
                        f"Adding report. Report type: {report_type}, url: {download_url}, "
                        f"original email: {original_email}."
                    )
                    available_reports.append({"type": report_type, "url": download_url, "id": message_id})
                else:
                    logging.warning(
                        f'Excluding file "{filename}.{extension}", as it does not have ".tsv" extension. '
                        f"Original email: {original_email}"
                    )

            return available_reports

        @task
        def download(available_reports: List[dict], **context) -> List[dict]:
            """Downloads the reports from the GMAIL API. A release is created for each unique release date.
            Upoads the reports to GCS

            :returns: List of unique releases
            """

            # Download reports and determine dates
            available_releases = {}
            for report in available_reports:
                # Download report to temporary file
                tmp_download_path = NamedTemporaryFile().name
                download_report(report, download_path=tmp_download_path)
                start_date, end_date = get_release_date(tmp_download_path)

                # Create temporary release and move report to correct path
                release = MuseRelease(
                    dag_id=dag_id,
                    run_id=context["run_id"],
                    data_interval_start=start_date,
                    data_interval_end=end_date.add(days=1).start_of("month"),
                    partition_date=end_date,
                    reports=[report],
                )
                download_path = (
                    release.download_path if report["type"] == "country" else release.download_institution_path
                )
                shutil.move(tmp_download_path, download_path)

                # Add reports to list with available releases
                release_date = release.partition_date.format("YYYYMMDD")
                try:
                    available_releases[release_date].append(report)
                except KeyError:
                    available_releases[release_date] = [report]

            # Generate the release for each release date
            releases = []
            for release_date, reports in available_releases.items():
                partition_date = pendulum.parse(release_date)
                data_interval_start = partition_date.start_of("month")
                data_interval_end = partition_date.add(days=1).start_of("month")
                releases.append(
                    MuseRelease(
                        dag_id=dag_id,
                        run_id=context["run_id"],
                        partition_date=partition_date,
                        data_interval_start=data_interval_start,
                        data_interval_end=data_interval_end,
                        reports=reports,
                    )
                )

            # Upload to GCS
            for release in releases:
                success = gcs_upload_files(
                    bucket_name=cloud_workspace.download_bucket,
                    file_paths=[release.download_institution_path, release.download_path],
                )
                set_task_state(success, context["ti"].task_id, release=release)

            return [r.to_dict() for r in releases]

        @task_group(group_id="process_release")
        def process_release(data):
            @task
            def transform(release: List[dict], **context) -> None:
                """Task to transform the muse data and upload to cloud storage"""

                release = MuseRelease.from_dict(release)
                # Download files
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_blob_name,
                    file_path=release.download_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file: {release.download_blob_name}")

                # Check the file encoding so we read it in properly
                csv_file = os.path.splitext(release.download_path)[0]
                with gzip.open(release.download_path, "rb") as gzf:
                    rawdata = gzf.read(1000000)  # read up to 1MB
                    encoding = chardet.detect(rawdata)["encoding"]
                    logging.ingo(f"Detected encoding: {encoding}")
                    gzf.seek(0)  # Go back to start of file
                    with open(csv_file, "wb") as f:
                        shutil.copyfileobj(gzf, f)

                with open(csv_file, "r", encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    data = [row for row in reader]
                data = muse_data_transform(data)
                save_jsonl_gz(release.transform_path, data)

                success = gcs_upload_file(
                    bucket_name=cloud_workspace.transform_bucket,
                    file_path=release.transform_path,
                    blob_name=release.transform_blob_name,
                )
                set_task_state(success, context["ti"].task_id, release=release)

            @task
            def bq_load(release: dict, **context) -> None:
                """Load the transfromed data into bigquery"""
                release = MuseRelease.from_dict(release)
                bq_create_dataset(
                    project_id=cloud_workspace.project_id,
                    dataset_id=data_partner.bq_dataset_id,
                    location=cloud_workspace.data_location,
                    description=bq_dataset_description,
                )

                # Load each transformed release
                uri = gcs_blob_uri(cloud_workspace.transform_bucket, release.transform_blob_name)
                table_id = bq_table_id(
                    cloud_workspace.project_id, data_partner.bq_dataset_id, data_partner.bq_table_name
                )
                client = Client(project=cloud_workspace.project_id)
                success = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=data_partner.schema_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    table_description=bq_table_description,
                    partition=True,
                    partition_type=TimePartitioningType.MONTH,
                    write_disposition=WriteDisposition.WRITE_APPEND,
                    partition_field=DATE_PARTITION_FIELD,
                    ignore_unknown_values=True,
                    client=client,
                )
                set_task_state(success, context["ti"].task_id, release=release)

            @task
            def add_new_dataset_releases(release: dict, **context) -> None:
                """Adds release information to API."""

                release = MuseRelease.from_dict(release)
                client = Client(project=cloud_workspace.project_id)
                api = DatasetAPI(
                    bq_project_id=cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id, client=client
                )
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    entity_id="muse",
                    dag_run_id=release.run_id,
                    created=pendulum.now(),
                    modified=pendulum.now(),
                    data_interval_start=release.data_interval_start,
                    data_interval_end=release.data_interval_end,
                    partition_date=release.partition_date,
                )
                api.add_dataset_release(dataset_release)

            @task
            def cleanup_workflow(release: dict, **context) -> None:
                """Delete all files and folders associated with this release."""
                release = MuseRelease.from_dict(release)
                cleanup(dag_id, workflow_folder=release.workflow_folder)

            transform(data) >> bq_load(data) >> add_new_dataset_releases(data) >> cleanup_workflow(data)

        # Define DAG tasks
        task_check_dependencies = check_dependencies(airflow_conns=[gmail_api_conn_id], start_date=start_date)
        xcom_reports = list_reports()
        xcom_releases = download(xcom_reports)
        process_release_task_group = process_release.expand(data=xcom_releases)

        (task_check_dependencies >> xcom_reports >> xcom_releases >> process_release_task_group)

    return muse()


def create_gmail_service() -> Resource:
    """Build the gmail service.

    :return: Gmail service instance
    """
    gmail_api_conn = BaseHook.get_connection("gmail_api")
    scopes = ["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"]
    creds = Credentials.from_authorized_user_info(gmail_api_conn.extra_dejson, scopes=scopes)

    service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    return service


def muse_data_transform(data: List[dict]) -> List[dict]:
    """Process a muse dataset. Expect a list of dictionaries where each entry is a row of the data

    :param data: The muse dataset
    :return: The transformed dataset"""
    transformed_data = []
    for row in data:
        transformed_data.extend(muse_row_transform(row))
    return transformed_data


def muse_row_transform(row: dict) -> List[dict]:
    """Process a row of muse data. Will always return a list, even if there is only one item in it

    :param row: The muse data row to transform
    :return: The transformed row(s) as a list"""
    # Add the release date - the last day of the month
    row[DATE_PARTITION_FIELD] = str(pendulum.Date(int(row["year"]), int(row["month"]), 1).end_of("month"))

    # Process ISBNS. In some cases we are given many isbns. Not all are ISBN13s.
    valid_isbns = []
    for i in row["isbns"].split(","):
        if len(i) == 13:
            valid_isbns.append(i)
    valid_isbns = list(set(valid_isbns))  # Drop duplicate isbns
    del row["isbns"]

    if len(valid_isbns) == 0:
        logging.warning(f"Row has no valid ISBNs, will return empty row: {row}")
        return []

    # Dump the transformed row to a list. One for each unique ISBN
    rows = []
    for i in valid_isbns:
        rows.append({"isbn": i, **row})

    return rows
