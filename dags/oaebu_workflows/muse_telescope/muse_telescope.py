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
from typing import List, Union
import shutil
import gzip
import csv
import base64
from tempfile import NamedTemporaryFile

import chardet
import pendulum
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowSkipException
from google.cloud.bigquery import SourceFormat, WriteDisposition, Client
from google.cloud.bigquery.table import TimePartitioningType
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition, Client
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import save_jsonl_gz
from observatory_platform.google.gcs import (
    gcs_blob_name_from_path,
    gcs_upload_file,
    gcs_blob_uri,
    gcs_download_blob,
)
from observatory_platform.google.bigquery import bq_load_table, bq_create_dataset, bq_table_id
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.airflow import on_failure_callback


class MuseRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        message_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
    ):
        """Create a MuseRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The airflow run ID
        :param message_id: The ID of the message (email) containing the report
        :param data_interval_start: The beginning of the data interval
        :param data_interval_end: The end of the data interval
        :param partition_date: The release/partition date
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.message_id = message_id
        self.partition_date = partition_date
        self.download_file_name = "muse_download.csv.gz"
        self.transform_file_name = "muse_transform.jsonl.gz"

    @property
    def download_path(self):
        return os.path.join(self.download_folder, self.download_file_name)

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
        return MuseRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            message_id=dict_["message_id"],
            data_interval_start=pendulum.parse(dict_["data_interval_start"]),
            data_interval_end=pendulum.parse(dict_["data_interval_end"]),
            partition_date=pendulum.parse(dict_["partition_date"]),
        )

    def to_dict(self):
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "message_id": self.message_id,
            "data_interval_start": self.data_interval_start.to_date_string(),
            "data_interval_end": self.data_interval_end.to_date_string(),
            "partition_date": self.partition_date.to_date_string(),
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    publisher_subject_line: str,
    email_sender: str = "muse_analytics@jh.edu.au",
    processed_label: str = "muse_processed",
    date_parition_field: str = "release_date",
    data_partner: Union[str, OaebuPartner] = "muse",
    bq_dataset_description: str = "MUSE dataset",
    bq_table_description: str = "Muse metrics",
    api_bq_dataset_id: str = "dataset_api",
    gmail_api_conn_id: str = "gmail_api",
    catchup: bool = False,
    schedule: str = "15 0 10 * *",  # Run on the 10th of every month at 0015 UTC
    start_date: pendulum.DateTime = pendulum.datetime(2022, 4, 1),  # Earliest available data
    max_active_runs: int = 1,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """The Muse Telescope
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param publisher_subject_line: The expected subject line of the email
    :param email_sender: The sending email address of the email
    :param date_partition_field: The field name to add and use as a YYYYMM table partition
    :param data_partner: The name of the data partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_bq_dataset_id: The name of the Bigquery dataset to store the API release(s)
    :param catchup: Whether to catchup the DAG or not
    :param schedule: The schedule interval of the DAG
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
        """Muse Dag

        For a data partner, will search emails for messages from the designated email sender only messages with the
        expected subject line and those that do not already have the 'processed' label will be considered. For each
        valid email found will download the attachement and create a new release object.
        Each release is processed concurrently, transformed, uploaded to GCloud storage, then loaded into Bigquery as
        part of a partitioned table. Once loading is complete, the 'processed' label is added to the original
        email/message so that it is ignored in future runs.
        """

        @task
        def create_releases(**context: dict) -> List[dict]:
            """List the available releases by going through the messages of a gmail account and looking for a specific pattern.

            If a message has been processed previously it has a specific label, messages with this label will be skipped.
            The message should include a download url. The head of this download url contains the filename, from which the
            release date and publisher can be derived.

            Lists all Jstor releases for a given month and publishes their report_type, download_url and
            release_date's as an XCom.

            :return: A list if dictionaries representing the messages with reports. Each has keys "type", "url" and "id".
            """

            # Get all of the viable reports for this publisher
            list_params = {
                "userId": "me",
                "q": f"-label:{processed_label} subject:{publisher_subject_line} from:{email_sender}",
                "labelIds": ["INBOX"],
                "maxResults": 500,
            }
            service = create_gmail_service()
            messages = get_messages(service, list_params)
            logging.info(f"Found {len(messages)} messages to process")
            if len(messages) == 0:
                raise AirflowSkipException("No messages to process. Skipping downstream tasks")

            releases = []
            for msg in messages:
                attachment_ids = get_message_attachement_ids(service, msg["id"])
                if len(attachment_ids) != 1:
                    raise AirflowException(f"Expected 1 attachment in message, instead got {len(attachment_ids)}")
                attachment_id = attachment_ids[0]

                tmp_download_path = NamedTemporaryFile(delete=False).name
                download_attachment(service, msg["id"], attachment_id, tmp_download_path)
                release_date = get_release_date_from_report(tmp_download_path)
                if not release_date:
                    raise AirflowException(f"Could not determine release date for report. Cannot proceed")

                release = MuseRelease(
                    dag_id=dag_id,
                    run_id=context["run_id"],
                    message_id=msg["id"],
                    data_interval_start=release_date.start_of("month"),
                    data_interval_end=release_date.end_of("month"),
                    partition_date=release_date,
                )
                shutil.copy(tmp_download_path, release.download_path)  # Move report to the release's download path
                success = gcs_upload_file(
                    bucket_name=cloud_workspace.download_bucket,
                    file_path=release.download_path,
                    blob_name=release.download_blob_name,
                )
                set_task_state(success, context["ti"].task_id, release=release)
                releases.append(release.to_dict())

            return releases

        @task_group(group_id="process_release")
        def process_release(data):
            @task
            def transform(release: List[dict], **context) -> None:
                """Task to transform the muse data and upload to cloud storage"""
                release = MuseRelease.from_dict(release)
                # Download files from GCS
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_blob_name,
                    file_path=release.download_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file: {release.download_blob_name}")

                data = read_gzipped_report(release.download_path)
                data = muse_data_transform(data)
                save_jsonl_gz(release.transform_path, data)

                # Upload transformed to GCS
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
                    partition_field=date_parition_field,
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
            def add_label(release: dict, **context) -> None:
                """Adds the processed label name to the message (email) so that it is not processed again"""
                release = MuseRelease.from_dict(release)
                service = create_gmail_service()
                label_id = get_label_id(service=service, label_name=processed_label)
                success = add_label_to_message(service=service, message_id=release.message_id, label_id=label_id)
                set_task_state(success, context["ti"].task_id, release=release)

            @task
            def cleanup_workflow(release: dict, **context) -> None:
                """Delete all files and folders associated with this release."""
                release = MuseRelease.from_dict(release)
                cleanup(dag_id, workflow_folder=release.workflow_folder)

            (
                transform(data)
                >> bq_load(data)
                >> add_new_dataset_releases(data)
                >> add_label(data)
                >> cleanup_workflow(data)
            )

        # Define DAG tasks
        task_check_dependencies = check_dependencies(airflow_conns=[gmail_api_conn_id])
        xcom_releases = create_releases()
        process_release_task_group = process_release.expand(data=xcom_releases)

        (task_check_dependencies >> xcom_releases >> process_release_task_group)

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


def muse_row_transform(row: dict, date_partition_field: str = "release_date") -> List[dict]:
    """Process a row of muse data. Will always return a list, even if there is only one item in it.
    The 'isbns' field is a string of comma-delimited isbn13s and isbn10s. This function will drop isbn10s and duplicate
    the data for each isbn

    :param row: The muse data row to transform
    :param date_partition_field: The added field that will hold the year/month to partition on
    :return: The transformed row(s) as a list"""
    # Add the release date - the last day of the month
    row[date_partition_field] = str(pendulum.Date(int(row["year"]), int(row["month"]), 1).end_of("month"))

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


# TODO: put this in some kind of common library. It's used in JSTOR as well
def download_attachment(service: Resource, message_id: str, attachment_id: str, download_path: str) -> None:
    """Download report from url to a file.

    :param download_path: Path to download data to
    """
    logging.info(f"Downloading report: {message_id} to: {download_path}")
    attachment = (
        service.users().messages().attachments().get(userId="me", messageId=message_id, id=attachment_id).execute()
    )
    data = attachment["data"]
    file_data = base64.urlsafe_b64decode(data)
    with open(download_path, "wb") as f:
        f.write(file_data)


def read_gzipped_report(gz_path: str) -> List[dict]:
    """Reads a gzipped csv file into a list of dictionaries. Automatically determines the encoding

    :param gz_path: The filepath of the gzipped csv file
    :return: The contents of the gzipped file as a list of dictionaries"""
    # Check the file encoding so we read it in properly
    tmp_file = NamedTemporaryFile().name
    with gzip.open(gz_path, "rb") as gzf:
        rawdata = gzf.read(1000000)  # read up to 1MB
        encoding = chardet.detect(rawdata)["encoding"]
        print(f"Detected encoding: {encoding}")
        gzf.seek(0)  # Go back to start of file
        with open(tmp_file, "wb") as f:
            shutil.copyfileobj(gzf, f)

    with open(tmp_file, "r", encoding=encoding) as f:
        reader = csv.DictReader(f)
        data = [row for row in reader]
    return data


def get_release_date_from_report(gz_path: str) -> Union[pendulum.DateTime, None]:
    """Reads a gziped report and determines the release date from it

    :param gz_path: The filepath of the gzipped csv file
    :return: Returns the release date if it exists, otherwise returns None
    """
    data = read_gzipped_report(gz_path)
    year = data[0].get("year")
    month = data[0].get("month")
    if not year or not month:
        return None

    # Check that all year/months are the same. Otherwise the report is invalid.
    for row in data:
        if not row.get("year") == year or not row.get("month") == month:
            raise RuntimeError(
                f"Inconsistent Year/Month found in report: Expected {year}/{month}, found {row.get('year')}/{row.get('month')}"
            )

    return pendulum.datetime(year=int(year), month=int(month), day=1).end_of("month").start_of("day")


# TODO: put this in some kind of common library. It's used in JSTOR as well
def get_label_id(service: Resource, label_name: str) -> str:
    """Get the id of a label based on the label name.

    :param label_name: The name of the label
    :return: The label id
    """
    existing_labels = service.users().labels().list(userId="me").execute()["labels"]
    label_id = [label["id"] for label in existing_labels if label["name"] == label_name]
    if label_id:
        label_id = label_id[0]
    else:
        # create label
        label_body = {
            "name": label_name,
            "messageListVisibility": "show",
            "labelListVisibility": "labelShow",
            "type": "user",
        }
        result = service.users().labels().create(userId="me", body=label_body).execute()
        label_id = result["id"]
    return label_id


# TODO: put this in some kind of common library. It's used in JSTOR as well
def get_messages(service: Resource, list_params: dict) -> List[dict]:
    """Get messages from the Gmail API.

    :param list_params: The parameters that will be passed to the Gmail API.
    """
    first_query = True
    next_page_token = None
    messages = []
    while next_page_token or first_query:
        # Set the next page token if it isn't the first query
        if not first_query:
            list_params["pageToken"] = next_page_token
        first_query = False

        # Get the results
        results = service.users().messages().list(**list_params).execute()
        next_page_token = results.get("nextPageToken")
        if results.get("messages"):
            messages += results["messages"]
    return messages


# TODO: put this in some kind of common library. It's used in JSTOR as well
def get_message_attachement_ids(service: Resource, message_id: str) -> List[str]:
    """Get attachement IDs from a message

    :param service: The Gmail service client
    :param message_id: The message ID to get the attachments of
    """

    msg_detail = service.users().messages().get(userId="me", id=message_id).execute()
    parts = msg_detail.get("payload", {}).get("parts", [])

    ids = []
    for part in parts:
        filename = part.get("filename")
        body = part.get("body", {})
        if filename:  # Only parts with a filename are attachments
            ids.append(body["attachmentId"])

    return ids


def add_label_to_message(
    service: Resource,
    message_id: str,
    label_id: str,
) -> bool:
    """Adds a label name to a gmail message.

    :param service: The Gmail service resource
    :param message_id: The ID of the message to add the label to
    :param label_id: The id of the label to add the message
    :return: True if successful, False otherwise
    """
    body = {"addLabelIds": [label_id]}
    response = service.users().messages().modify(userId="me", id=message_id, body=body).execute()
    if response and label_id in response.get("labelIds", []):
        logging.info(f"Added label_id '{label_id}' to GMAIL message, message_id: {message_id}")
        return True

    logging.warning(f"Could not add label_id '{label_id}' to GMAIL message, message_id: {message_id}")
    return False
