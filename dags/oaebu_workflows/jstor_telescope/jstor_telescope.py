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
#
# Author: Aniek Roelofs, Keegan Smith

from __future__ import annotations

import base64
import csv
import logging
import os
import shutil
import re
from collections import OrderedDict
from typing import List, Union, Tuple, Literal, Optional, Any
from tempfile import NamedTemporaryFile
from abc import ABC, abstractmethod

import pendulum
import requests
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task, task_group
from bs4 import BeautifulSoup, SoupStrainer
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition, Client
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed

from oaebu_workflows.config import oaebu_user_agent_header
from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import save_jsonl_gz
from observatory_platform.url_utils import retry_get_url
from observatory_platform.google.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path, gcs_download_blob
from observatory_platform.google.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.files import add_partition_date, convert
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup

JSTOR_PROCESSED_LABEL_NAME = "processed_report"

# download settings
JSTOR_MAX_ATTEMPTS = 3
JSTOR_FIXED_WAIT = 20  # seconds
JSTOR_MAX_WAIT_TIME = 60 * 10  # seconds
JSTOR_EXP_BASE = 3
JSTOR_MULTIPLIER = 10
JSTOR_WAIT_FN = wait_fixed(JSTOR_FIXED_WAIT) + wait_exponential(
    multiplier=JSTOR_MULTIPLIER, exp_base=JSTOR_EXP_BASE, max=JSTOR_MAX_WAIT_TIME
)


class JstorRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
        reports: List[dict],
    ):
        """Construct a JstorRelease.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param data_interval_start: The beginning of the data interval
        :param data_interval_end: The end of the data interval
        :param partition_date: the partition date, corresponds to the last day of the month being processed.
        :param reports: list with report_type (country or institution) and url of reports
        """
        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            partition_date=partition_date,
        )
        self.reports = reports
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.download_country_file_name = "country.tsv"
        self.download_institution_file_name = "institution.tsv"
        self.transform_country_file_name = "country.jsonl.gz"
        self.transform_institution_file_name = "institution.jsonl.gz"

    @property
    def download_country_path(self):
        return os.path.join(self.download_folder, self.download_country_file_name)

    @property
    def download_institution_path(self):
        return os.path.join(self.download_folder, self.download_institution_file_name)

    @property
    def transform_country_path(self):
        return os.path.join(self.transform_folder, self.transform_country_file_name)

    @property
    def transform_institution_path(self):
        return os.path.join(self.transform_folder, self.transform_institution_file_name)

    @property
    def download_country_blob_name(self):
        return gcs_blob_name_from_path(self.download_country_path)

    @property
    def download_institution_blob_name(self):
        return gcs_blob_name_from_path(self.download_institution_path)

    @property
    def transform_country_blob_name(self):
        return gcs_blob_name_from_path(self.transform_country_path)

    @property
    def transform_institution_blob_name(self):
        return gcs_blob_name_from_path(self.transform_institution_path)

    @staticmethod
    def from_dict(dict_: dict):
        return JstorRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            data_interval_start=pendulum.parse(dict_["data_interval_start"]),
            data_interval_end=pendulum.parse(dict_["data_interval_end"]),
            partition_date=pendulum.parse(dict_["partition_date"]),
            reports=dict_["reports"],
        )

    def to_dict(self):
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "data_interval_start": self.data_interval_start.to_date_string(),
            "data_interval_end": self.data_interval_end.to_date_string(),
            "partition_date": self.partition_date.to_date_string(),
            "reports": self.reports,
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    entity_id: str,
    entity_type: Literal["publisher", "collection"] = "publisher",
    country_partner: Union[str, OaebuPartner] = "jstor_country",
    institution_partner: Union[str, OaebuPartner] = "jstor_institution",
    bq_dataset_description: str = "Data from JSTOR sources",
    bq_country_table_description: Optional[str] = None,
    bq_institution_table_description: Optional[str] = None,
    api_dataset_id: str = "jstor",
    gmail_api_conn_id: str = "gmail_api",
    catchup: bool = False,
    schedule: str = "0 0 4 * *",  # 4th day of every month
    start_date: pendulum.DateTime = pendulum.datetime(2016, 10, 1),
):
    """Construct a Jstor DAG.
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param entity_id: The ID of the publisher for this DAG
    :param entity_type: Whether this entity should be treated as a publisher or a collection
    :param country_partner: The name of the country partner
    :param institution_partner: The name of the institution partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_country_table_description: Description for the BigQuery JSTOR country table
    :param bq_institution_table_description: Description for the BigQuery JSTOR institution table
    :param api_dataset_id: The ID to store the dataset release in the API
    :param gmail_api_conn_id: Airflow connection ID for the Gmail API
    :param catchup: Whether to catchup the DAG or not
    :param max_active_runs: The maximum number of DAG runs that can be run concurrently
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    """

    country_partner = partner_from_str(country_partner)
    institution_partner = partner_from_str(institution_partner)

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["oaebu"],
        default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=5)},
    )
    def jstor():
        @task
        def list_reports(**context) -> List[dict]:
            """Lists all Jstor releases for a given month and publishes their report_type, download_url and
            release_date's as an XCom.

            :raises AirflowSkipException: Raised if there are no available reports
            :return: A list of available reports
            """

            # Get the reports from GMAIL API
            api = make_jstor_api(entity_type, entity_id)
            available_reports = api.list_reports()
            if not len(available_reports) > 0:
                raise AirflowSkipException("No reports available. Skipping downstream DAG taks.")
            return available_reports

        @task
        def download(available_reports: List[dict], **context) -> List[dict]:
            """Downloads the reports from the GMAIL API. A release is created for each unique release date.
            Upoads the reports to GCS

            :returns: List of unique releases
            """

            # Download reports and determine dates
            available_releases = {}
            api = make_jstor_api(entity_type, entity_id)
            for report in available_reports:
                # Download report to temporary file
                tmp_download_path = NamedTemporaryFile().name
                api.download_report(report, download_path=tmp_download_path)
                start_date, end_date = api.get_release_date(tmp_download_path)

                # Create temporary release and move report to correct path
                release = JstorRelease(
                    dag_id=dag_id,
                    run_id=context["run_id"],
                    data_interval_start=start_date,
                    data_interval_end=end_date.add(days=1).start_of("month"),
                    partition_date=end_date,
                    reports=[report],
                )
                download_path = (
                    release.download_country_path if report["type"] == "country" else release.download_institution_path
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
                    JstorRelease(
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
                    file_paths=[release.download_institution_path, release.download_country_path],
                )
                set_task_state(success, context["ti"].task_id, release=release)

            return [r.to_dict() for r in releases]

        @task_group(group_id="process_release")
        def process_release(data):
            @task
            def transform(release: dict, **context) -> None:
                """Task to transform the Jstor releases for a given month."""

                release = JstorRelease(release)
                api = make_jstor_api(entity_type, entity_id)
                # Download files from GCS
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_country_blob_name,
                    file_path=release.download_country_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file: {release.download_country_blob_name}")
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_institution_blob_name,
                    file_path=release.download_institution_path,
                )
                if not success:
                    raise FileNotFoundError(f"Error downloading file: {release.download_institution_blob_name}")

                api.transform_reports(
                    download_country=release.download_country_path,
                    download_institution=release.download_institution_path,
                    transform_country=release.transform_country_path,
                    transform_institution=release.transform_institution_path,
                    partition_date=release.partition_date,
                )

                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket,
                    file_paths=[release.transform_country_path, release.transform_institution_path],
                )
                set_task_state(success, context["ti"].task_id, release=release)

            @task
            def bq_load(release: dict, **context) -> None:
                """Loads the sales and traffic data into BigQuery"""

                release = JstorRelease(release)
                client = Client(project=cloud_workspace.project_id)
                for partner, table_description, file_path in [
                    (country_partner, bq_country_table_description, release.transform_country_path),
                    (
                        institution_partner,
                        bq_institution_table_description,
                        release.transform_institution_path,
                    ),
                ]:
                    bq_create_dataset(
                        project_id=cloud_workspace.project_id,
                        dataset_id=partner.bq_dataset_id,
                        location=cloud_workspace.data_location,
                        description=bq_dataset_description,
                    )
                    uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(file_path))
                    table_id = bq_table_id(cloud_workspace.project_id, partner.bq_dataset_id, partner.bq_table_name)
                    state = bq_load_table(
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
                    set_task_state(state, context["ti"].task_id, release=release)

            @task
            def add_new_dataset_releases(release: dict, **context) -> None:
                """Adds release information to API."""

                release = JstorRelease(release)
                client = Client(project=cloud_workspace.project_id)
                api = DatasetAPI(project_id=cloud_workspace.project_id, client=client)
                api.seed_db()
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    dataset_id=api_dataset_id,
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
                """Delete all files, folders and XComs associated with this release.
                Assign a label to the gmail messages that have been processed."""

                release = JstorRelease(release)
                api = make_jstor_api(entity_type, entity_id)
                cleanup(
                    dag_id=dag_id,
                    execution_date=context["execution_date"],
                    workflow_folder=release.workflow_folder,
                )
                success = api.add_labels(release.reports)
                set_task_state(success, context["ti"].task_id, release=release)

            transform(data) >> bq_load(data) >> add_new_dataset_releases(data) >> cleanup_workflow(data)

        # Define DAG tasks
        task_check_dependencies = check_dependencies(airflow_conns=[gmail_api_conn_id], start_date=start_date)
        xcom_reports = list_reports()
        xcom_releases = download(xcom_reports)
        process_release_task_group = process_release.expand(data=xcom_releases)

        (task_check_dependencies >> xcom_reports >> xcom_releases >> process_release_task_group)

    return jstor()


def create_gmail_service() -> Resource:
    """Build the gmail service.

    :return: Gmail service instance
    """
    gmail_api_conn = BaseHook.get_connection("gmail_api")
    scopes = ["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"]
    creds = Credentials.from_authorized_user_info(gmail_api_conn.extra_dejson, scopes=scopes)

    service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    return service


def make_jstor_api(entity_type: Literal["publisher", "collection"], entity_id: str):
    """Create the Jstor API Instance.

    :param entity_type: The entity type. Should be either 'publisher' or 'collection'.
    :param entity_id: The entity id.
    :return: The Jstor API instance
    """

    service = create_gmail_service()
    if entity_type == "publisher":
        jstor_api = JstorPublishersAPI(service, entity_id)
    elif entity_type == "collection":
        jstor_api = JstorCollectionsAPI(service, entity_id)
    else:
        raise AirflowException(f"Entity type must be 'publisher' or 'collection', got {entity_type}.")
    return jstor_api


class JstorAPI(ABC):
    def __init__(self, service: Any, entity_id: str):
        self.service = service
        self.entity_id = entity_id

    def get_messages(self, list_params: dict) -> List[dict]:
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
            results = self.service.users().messages().list(**list_params).execute()
            next_page_token = results.get("nextPageToken")
            messages += results["messages"]
        return messages

    def get_label_id(self, label_name: str) -> str:
        """Get the id of a label based on the label name.

        :param label_name: The name of the label
        :return: The label id
        """
        existing_labels = self.service.users().labels().list(userId="me").execute()["labels"]
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
            result = self.service.users().labels().create(userId="me", body=label_body).execute()
            label_id = result["id"]
        return label_id

    @abstractmethod
    def list_reports(self) -> List[dict]:
        pass

    @abstractmethod
    def get_release_date(self, report_path: str) -> tuple[pendulum.DateTime, pendulum.DateTime]:
        pass

    @abstractmethod
    def download_report(self, report: dict, download_path: str) -> None:
        pass

    @abstractmethod
    def transform_reports(
        self,
        download_country: str,
        download_institution: str,
        transform_country: str,
        transform_institution: str,
        partition_date: pendulum.DateTime,
    ) -> None:
        pass

    @abstractmethod
    def add_labels(self, reports: List[dict]) -> bool:
        pass


class JstorPublishersAPI(JstorAPI):
    def __init__(self, service: Any, entity_id: str):
        super().__init__(service, entity_id)

    def list_reports(self) -> List[dict]:
        """List the available releases by going through the messages of a gmail account and looking for a specific pattern.

        If a message has been processed previously it has a specific label, messages with this label will be skipped.
        The message should include a download url. The head of this download url contains the filename, from which the
        release date and publisher can be derived.

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

    def download_report(self, report: dict, download_path: str) -> None:
        """Download report from url to a file.

        :param report: The report info. Should contain the "url" key
        :param download_path: Path to download data to
        """
        url = report.get("url")
        if not url:
            raise KeyError(f"'url' not found in report: {report}")

        logging.info(f"Downloading report: {url} to: {download_path}")
        response = retry_get_url(
            url, headers=oaebu_user_agent_header(), wait=JSTOR_WAIT_FN, num_retries=JSTOR_MAX_ATTEMPTS
        )
        content = response.content.decode("utf-8")
        with open(download_path, "w") as f:
            f.write(content)

    @retry(stop=stop_after_attempt(JSTOR_MAX_ATTEMPTS), reraise=True, wait=JSTOR_WAIT_FN)
    def get_header_info(self, url: str) -> Tuple[str, str]:
        """Get header info from url and parse for filename and extension of file.

        :param url: Download url
        :return: Filename and file extension
        """
        logging.info(
            f"Getting HEAD of report: {url}, "
            f'attempt: {self.get_header_info.retry.statistics["attempt_number"]}, '
            f'idle for: {self.get_header_info.retry.statistics["idle_for"]}'
        )
        response = requests.head(url, allow_redirects=True, headers=oaebu_user_agent_header())
        if response.status_code != 200:
            raise AirflowException(
                f"Could not get HEAD of report download url, reason: {response.reason}, "
                f"status_code: {response.status_code}"
            )
        filename, extension = response.headers["Content-Disposition"].split("=")[1].split(".")
        return filename, extension

    def get_release_date(self, report_path: str) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        """Get the release date from the "Reporting_Period" part of the header.
        Also checks if the reports contains data from exactly one month.

        :param report_path: The path to the JSTOR report
        :return: The start and end of the release month
        """

        # Check if report has header in old or new format based on the first line
        with open(report_path, "r") as tsv_file:
            first_line = tsv_file.readline()

        # Process report with the old header
        if not "Report_Name" in first_line:
            return self.get_release_date_deprecated(report_path)

        # Process report with new header
        start_date = None
        end_date = None
        with open(report_path, "r") as tsv_file:
            for line in tsv_file:
                line = line.strip("\n").split("\t")
                if line[0] == "Reporting_Period":
                    start_date = pendulum.parse(line[1].split(" ")[0])
                    end_date = pendulum.parse(line[1].split(" ")[-1])
                    break

        if not start_date or not end_date:
            raise RuntimeError(f"Could not parse start and end date from report: {report_path}")

        # Check that month of start and end date are the same
        if start_date.end_of("month").start_of("day") != end_date:
            raise AirflowException(
                f"Report contains data that is not from exactly 1 month, start date: {start_date}, end date: {end_date}"
            )
        return start_date, end_date

    def get_release_date_deprecated(self, report_path: str) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        """This function is deprecated, because the headers for the reports have changed since 2021-10-01.
        It might still be used for reports that were created before this date and have not been processed yet.
        Get the release date from the "Usage Month" column in the first row of the report.
        Also checks if the reports contains data from the same month only.

        :param report_path: The path to the JSTOR report
        :return: The start and end dates
        """
        # Load report data into list of dicts
        with open(report_path) as tsv_file:
            csv_list = list(csv.DictReader(tsv_file, delimiter="\t"))

        # get the first and last usage month
        first_usage_month = csv_list[0]["Usage Month"]
        last_usage_month = csv_list[-1]["Usage Month"]

        # check that month in first and last row are the same
        if first_usage_month != last_usage_month:
            raise AirflowException(
                f"Report contains data from more than 1 month, start month: {first_usage_month}, "
                f"end month: {last_usage_month}"
            )

        # get the release date from the last usage month
        start_date = pendulum.from_format(first_usage_month, "YYYY-MM").start_of("month").start_of("day")
        end_date = pendulum.from_format(last_usage_month, "YYYY-MM").end_of("month").start_of("day")

        return start_date, end_date

    def transform_reports(
        self,
        download_country: str,
        download_institution: str,
        transform_country: str,
        transform_institution: str,
        partition_date: pendulum.DateTime,
    ) -> None:
        """Transform a Jstor release into json lines format and gzip the result._summary_

        :param download_country: The path to the country download report
        :param download_institution: The path to the institution download report
        :param transform_country: The path to write the transformed country file to
        :param transform_institution: The path to write the transformed institution file to
        :param partition_date: The partition/release date of this report
        """
        for download_file, transform_file in (
            [download_country, transform_country],
            [download_institution, transform_institution],
        ):
            release_column = partition_date.strftime("%b-%Y")  # e.g. Jan-2020
            usage_month = partition_date.strftime("%Y-%m")

            # Check if report has header in old or new format based on the first line
            results = []
            with open(download_file, "r") as tsv_file:
                first_line = tsv_file.readline()
                tsv_file.seek(0)
                if "Report_Name" in first_line:
                    line = None
                    while line != "\n":
                        line = next(tsv_file)
                report = list(csv.DictReader(tsv_file, delimiter="\t"))
            for row in report:
                transformed_row = OrderedDict((convert(k), v) for k, v in row.items())
                transformed_row.pop(release_column, None)
                if "Usage_Month" not in transformed_row:  # Newwer reports name it by month name
                    transformed_row["Usage_Month"] = usage_month
                if "Total_Item_Requests" not in transformed_row:  # Institution report names it "Reporting_Period_Total"
                    transformed_row["Total_Item_Requests"] = transformed_row.pop("Reporting_Period_Total")
                results.append(transformed_row)

            results = add_partition_date(
                results, partition_date, TimePartitioningType.MONTH, partition_field="release_date"
            )
            save_jsonl_gz(transform_file, results)

    def add_labels(self, reports: List[dict]) -> bool:
        """Adds the label name to all messages in the report

        :param reports: List of report info
        :return: True if successful, False otherwise
        """
        label_id = self.get_label_id(JSTOR_PROCESSED_LABEL_NAME)
        body = {"addLabelIds": [label_id]}
        for message in [report["id"] for report in reports]:
            response = self.service.users().messages().modify(userId="me", id=message, body=body).execute()
            try:
                message_id = response["id"]
                logging.info(f"Added label '{JSTOR_PROCESSED_LABEL_NAME}' to GMAIL message, message_id: {message_id}")
            except KeyError:
                return False
        return True


class JstorCollectionsAPI(JstorAPI):
    def __init__(self, service: Any, entity_id: str):
        super().__init__(service, entity_id)

    def list_reports(self) -> List[dict]:
        """List the available reports by going through the gmail messages from a specific sender.

        :return: A list if dictionaries representing the messages with reports as attachments.
            Each has keys "type", "attachment_id" and "id".
        """
        # List messages with specific query
        list_params = {
            "userId": "me",
            "q": f"-label:{JSTOR_PROCESSED_LABEL_NAME} from:grp_ithaka_data_intelligence@ithaka.org",
            "labelIds": ["INBOX"],
            "maxResults": 500,
        }
        available_reports = []
        country_regex = rf"^{self.entity_id}_Open_Country_Usage\.csv$"
        institution_regex = rf"^{self.entity_id}_Open_Institution_Usage\.csv$"
        for message_info in self.get_messages(list_params):
            message_id = message_info["id"]
            message = self.service.users().messages().get(userId="me", id=message_id).execute()

            # Messages without payloads should be ignored
            try:
                attachments = message["payload"]["parts"]
            except KeyError:
                continue

            # Get download filenames
            for attachment in attachments:
                if re.match(country_regex, attachment["filename"]):
                    report_type = "country"
                elif re.match(institution_regex, attachment["filename"]):
                    report_type = "institution"
                else:
                    continue
                attachment_id = attachment["body"]["attachmentId"]
                available_reports.append({"type": report_type, "attachment_id": attachment_id, "id": message_id})

        return available_reports

    def download_report(self, report: dict, download_path: str) -> None:
        """Download report from url to a file.

        :param report: The report info. Should contain the "id" and "attachement_id" keys
        :param download_path: Path to download data to
        """
        message_id = report.get("id")
        attachment_id = report.get("attachment_id")
        if not all((message_id, attachment_id)):
            raise KeyError(f"'id' and/or 'attachment_id' not found in report: {report}")

        logging.info(f"Downloading report: {message_id} to: {download_path}")
        attachment = (
            self.service.users()
            .messages()
            .attachments()
            .get(userId="me", messageId=message_id, id=attachment_id)
            .execute()
        )
        data = attachment["data"]
        file_data = base64.urlsafe_b64decode(data)
        with open(download_path, "wb") as f:
            f.write(file_data)

    def get_release_date(self, report_path: str) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
        """Get the release date from the report. This should be under the "Month, Year of monthdt" column
        Also checks if the reports contains data from exactly one month.

        :param report_path: The path to the JSTOR report
        :return: The start and end of the release month
        """
        with open(report_path) as f:
            report = list(csv.DictReader(f))
        dates = list(set([i["Month, Year of monthdt"] for i in report]))

        if len(dates) != 1:
            raise AirflowException(f"Report contains data that is not from exactly 1 month, dates: {dates}")

        month = pendulum.from_format(dates[0], "MMMM YYYY")  # eg. September 2023
        start_date = month.start_of("month").start_of("day")
        end_date = month.end_of("month").start_of("day")
        return start_date, end_date

    def transform_reports(
        self,
        download_country: str,
        download_institution: str,
        transform_country: str,
        transform_institution: str,
        partition_date: pendulum.DateTime,
    ) -> None:
        """Transform a Jstor release into json lines format and gzip the result._summary_

        :param download_country: The path to the country download report
        :param download_institution: The path to the institution download report
        :param transform_country: The path to write the transformed country file to
        :param transform_institution: The path to write the transformed institution file to
        :param partition_date: The partition/release date of this report
        """
        for entity, download_file, transform_file in (
            ["Country_Name", download_country, transform_country],
            ["Institution", download_institution, transform_institution],
        ):
            results = []
            with open(download_file, "r") as csv_file:
                report = list(csv.DictReader(csv_file))

            for row in report:
                row.pop("Month, Year of monthdt")
                row["Publisher"] = row.pop("publisher")
                row["Book_ID"] = row.pop("item_doi")
                row["Usage_Month"] = partition_date.strftime("%Y-%m")
                row[entity] = row.pop("\ufeffentity_name")
                row["Book_Title"] = row.pop("book_title")
                row["Authors"] = row.pop("authors")
                row["ISBN"] = row.pop("eisbn")
                row["eISBN"] = row["ISBN"]
                row["Total_Item_Requests"] = row.pop("total_item_requests")
                transformed_row = OrderedDict((convert(k), v) for k, v in row.items())
                results.append(transformed_row)

            results = add_partition_date(
                results, partition_date, TimePartitioningType.MONTH, partition_field="release_date"
            )
            save_jsonl_gz(transform_file, results)

    def add_labels(self, reports: List[dict]) -> bool:
        """Adds the label name to all messages in the report

        :param reports: List of report info
        :return: True if successful, False otherwise
        """
        label_id = self.get_label_id(JSTOR_PROCESSED_LABEL_NAME)
        body = {"addLabelIds": [label_id]}
        message = reports[0]["id"]  # Only one message for collections
        response = self.service.users().messages().modify(userId="me", id=message, body=body).execute()
        try:
            message_id = response["id"]
            logging.info(f"Added label '{JSTOR_PROCESSED_LABEL_NAME}' to GMAIL message, message_id: {message_id}")
        except KeyError:
            return False
        return True
