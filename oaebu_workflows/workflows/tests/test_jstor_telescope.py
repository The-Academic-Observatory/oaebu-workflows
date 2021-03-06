# Copyright 2020 Curtin University
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

import base64
import json
import os
from unittest.mock import patch

import httpretty
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.jstor_telescope import JstorRelease, JstorTelescope, get_label_id, get_release_date
from observatory.api.client.model.organisation import Organisation
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection
from airflow.utils.state import State


class TestJstorTelescope(ObservatoryTestCase):
    """Tests for the Jstor telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestJstorTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.organisation_name = "ANU Press"
        self.extra = {"publisher_id": "anupress"}
        self.host = "localhost"
        self.api_port = find_free_port()

        self.release_date = pendulum.parse("20210301").end_of("month")
        publisher_id = self.extra.get("publisher_id")
        self.country_report = {
            "path": test_fixtures_folder("jstor", "country_20210401.tsv"),
            "url": "https://www.jstor.org/admin/reports/download/249192019",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_{publisher_id}_PUBBCU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
            "download_hash": "1bad528f89b2d8df0846c47a58d7fb2e",
            "transform_hash": "9b197a54",
            "table_rows": 10,
        }
        self.institution_report = {
            "path": test_fixtures_folder("jstor", "institution_20210401.tsv"),
            "url": "https://www.jstor.org/admin/reports/download/129518301",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_{publisher_id}_PUBBIU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
            "download_hash": "793ee70d9102d8dca3cace65cb00ecc3",
            "transform_hash": "4a664f4d",
            "table_rows": 3,
        }
        self.wrong_publisher_report = {
            "path": test_fixtures_folder("jstor", "institution_20210401.tsv"),  # has to be valid path, but is not used
            "url": "https://www.jstor.org/admin/reports/download/12345",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_publisher_PUBBIU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
        }

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin Press"

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Jstor Telescope"
        workflow_type = WorkflowType(name=name, type_id=JstorTelescope.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name=self.org_name,
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=JstorTelescope.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Jstor Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the Jstor DAG has the correct structure.

        :return: None
        """
        organisation = Organisation(name=self.organisation_name)
        dag = JstorTelescope(organisation, self.extra.get("publisher_id")).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["list_reports"],
                "list_reports": ["download_reports"],
                "download_reports": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Jstor DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "jstor_telescope.py")
            self.assert_dag_load("jstor_curtin_press", dag_file)

    @patch("oaebu_workflows.workflows.jstor_telescope.build")
    @patch("oaebu_workflows.workflows.jstor_telescope.Credentials")
    def test_telescope(self, mock_account_credentials, mock_build):
        """Test the Jstor telescope end to end.

        :return: None.
        """

        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(
            create_http_mock_sequence(
                self.country_report["url"], self.institution_report["url"], self.wrong_publisher_report["url"]
            )
        )
        mock_build.return_value = build("gmail", "v1", http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        organisation = Organisation(
            name=self.organisation_name,
            project_id=self.project_id,
            download_bucket=env.download_bucket,
            transform_bucket=env.transform_bucket,
        )
        telescope = JstorTelescope(
            organisation=organisation, publisher_id=self.extra.get("publisher_id"), dataset_id=dataset_id, workflow_id=1
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.setup_connections(env)
            self.setup_api()
            with env.create_dag_run(dag, execution_date):
                # add gmail connection
                conn = Connection(
                    conn_id=AirflowConns.GMAIL_API,
                    uri="google-cloud-platform://?token=123&refresh_token=123"
                    "&client_id=123.apps.googleusercontent.com&client_secret=123",
                )
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                with httpretty.enabled():
                    for report in [self.country_report, self.institution_report, self.wrong_publisher_report]:
                        self.setup_mock_file_download(
                            report["url"], report["path"], headers=report["headers"], method=httpretty.HEAD
                        )
                    ti = env.run_task(telescope.list_reports.__name__)
                available_reports = ti.xcom_pull(
                    key=JstorTelescope.REPORTS_INFO, task_ids=telescope.list_reports.__name__, include_prior_dates=False
                )
                self.assertIsInstance(available_reports, list)
                expected_reports_info = [
                    {"type": "country", "url": self.country_report["url"], "id": "1788ec9e91f3de62"},
                    {"type": "institution", "url": self.institution_report["url"], "id": "1788ebe4ecbab055"},
                ]
                self.assertListEqual(expected_reports_info, available_reports)

                # Test download_reports task
                with httpretty.enabled():
                    for report in [self.country_report, self.institution_report]:
                        self.setup_mock_file_download(report["url"], report["path"], headers=report["headers"])
                    ti = env.run_task(telescope.download_reports.__name__)

                # use release info for other tasks
                available_releases = ti.xcom_pull(
                    key=JstorTelescope.RELEASE_INFO,
                    task_ids=telescope.download_reports.__name__,
                    include_prior_dates=False,
                )
                self.assertIsInstance(available_releases, dict)
                self.assertEqual(1, len(available_releases))
                for release_date, reports_info in available_releases.items():
                    self.assertEqual(self.release_date.date(), pendulum.parse(release_date).date())
                    self.assertIsInstance(reports_info, list)
                    self.assertListEqual(expected_reports_info, reports_info)
                release = JstorRelease(telescope.dag_id, pendulum.parse(release_date), reports_info, organisation)

                self.assertEqual(2, len(release.download_files))
                for file in release.download_files:
                    if "country" in file:
                        expected_file_hash = self.country_report["download_hash"]
                    else:
                        expected_file_hash = self.institution_report["download_hash"]
                    self.assert_file_integrity(file, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                for file in release.download_files:
                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(2, len(release.transform_files))
                for file in release.transform_files:
                    print(file)
                    if "country" in file:
                        expected_file_hash = self.country_report["transform_hash"]
                    else:
                        expected_file_hash = self.institution_report["transform_hash"]
                    self.assert_file_integrity(file, expected_file_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load_partition.__name__)
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                    if "country" in file:
                        expected_rows = self.country_report["table_rows"]
                    else:
                        expected_rows = self.institution_report["table_rows"]
                    self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    def test_get_label_id(self):
        """Test getting label id both when label already exists and does not exist yet.

        :return: None.
        """
        list_labels_no_match = {
            "labels": [
                {
                    "id": "CHAT",
                    "name": "CHAT",
                    "messageListVisibility": "hide",
                    "labelListVisibility": "labelHide",
                    "type": "system",
                },
                {"id": "SENT", "name": "SENT", "type": "system"},
            ]
        }
        create_label = {
            "id": "created_label",
            "name": JstorTelescope.PROCESSED_LABEL_NAME,
            "messageListVisibility": "show",
            "labelListVisibility": "labelShow",
        }
        list_labels_match = {
            "labels": [
                {
                    "id": "CHAT",
                    "name": "CHAT",
                    "messageListVisibility": "hide",
                    "labelListVisibility": "labelHide",
                    "type": "system",
                },
                {
                    "id": "existing_label",
                    "name": JstorTelescope.PROCESSED_LABEL_NAME,
                    "messageListVisibility": "show",
                    "labelListVisibility": "labelShow",
                    "type": "user",
                },
            ]
        }
        http = HttpMockSequence(
            [
                ({"status": "200"}, json.dumps(list_labels_no_match)),
                ({"status": "200"}, json.dumps(create_label)),
                ({"status": "200"}, json.dumps(list_labels_match)),
            ]
        )
        service = build("gmail", "v1", http=http)

        # call function without match for label, so label is created
        label_id = get_label_id(service, JstorTelescope.PROCESSED_LABEL_NAME)
        self.assertEqual("created_label", label_id)

        # call function with match for label
        label_id = get_label_id(service, JstorTelescope.PROCESSED_LABEL_NAME)
        self.assertEqual("existing_label", label_id)

    def test_get_release_date(self):
        """Test that the get_release_date returns the correct release date and raises an exception when dates are
        incorrect

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Test reports in new format with header
            new_report_content = (
                "Report_Name\tBook Usage by Country\nReport_ID\tPUB_BCU\nReport_Description\t"
                "Usage of your books on JSTOR by country.\nPublisher_Name\tPublisher 1\nReporting_Period\t"
                "{start_date} to {end_date}\nCreated\t2021-10-01\nCreated_By\tJSTOR"
            )
            old_report_content = (
                "Book Title\tUsage Month\nAddress source war after\t{start_date}\nNote spend " "government\t{end_date}"
            )
            reports = [
                {"file": "new_success.tsv", "start": "2020-01-01", "end": "2020-01-31"},
                {"file": "new_fail.tsv", "start": "2020-01-01", "end": "2020-02-01"},
                {"file": "old_success.tsv", "start": "2020-01", "end": "2020-01"},
                {"file": "old_fail.tsv", "start": "2020-01", "end": "2020-02"},
            ]

            for report in reports:
                with open(report["file"], "w") as f:
                    if report == reports[0] or report == reports[1]:
                        f.write(new_report_content.format(start_date=report["start"], end_date=report["end"]))
                    else:
                        f.write(old_report_content.format(start_date=report["start"], end_date=report["end"]))

            # Test new report is successful
            release_date = get_release_date(reports[0]["file"])
            self.assertEqual(pendulum.parse(reports[0]["end"]), release_date)
            # Test new report fails
            with self.assertRaises(AirflowException):
                get_release_date(reports[1]["file"])
            # Test old report is successful
            release_date = get_release_date(reports[2]["file"])
            self.assertEqual(pendulum.parse(reports[2]["end"]).end_of("month"), release_date)
            # Test old report fails
            with self.assertRaises(AirflowException):
                get_release_date(reports[3]["file"])


def create_http_mock_sequence(
    country_report_url: str, institution_report_url: str, wrong_publisher_report_url: str
) -> list:
    """Create a list with mocked http responses

    :param country_report_url: URL to country report
    :param institution_report_url: URL to institution report
    :param wrong_publisher_report_url: URL to report with a non-matching publisher id
    :return: List with http responses
    """
    list_labels = {
        "labels": [
            {
                "id": "CHAT",
                "name": "CHAT",
                "messageListVisibility": "hide",
                "labelListVisibility": "labelHide",
                "type": "system",
            },
            {
                "id": "Label_1",
                "name": JstorTelescope.PROCESSED_LABEL_NAME,
                "messageListVisibility": "show",
                "labelListVisibility": "labelShow",
                "type": "user",
            },
        ]
    }
    list_messages1 = {
        "messages": [
            {"id": "1788ec9e91f3de62", "threadId": "1788e9b0a848236a"},
        ],
        "resultSizeEstimate": 2,
        "nextPageToken": 1234,
    }
    list_messages2 = {
        "messages": [
            {"id": "1788ebe4ecbab055", "threadId": "1788e9b0a848236a"},
            {"id": "5621ayw3vjtag411", "threadId": "1788e9b0a848236a"},
        ],
        "resultSizeEstimate": 2,
    }
    get_message1 = {
        "id": "1788ec9e91f3de62",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service "
        "Account, Your usage report &quot;Book Usage by Country&quot; is now available to "
        "download. Download Completed Report",
        "payload": {
            "partId": "",
            "mimeType": "text/html",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {
                "size": 12313,
                "data": base64.urlsafe_b64encode(
                    f'<a href="{country_report_url}">Download Completed Report</a>'.encode()
                ).decode(),
            },
        },
        "sizeEstimate": 17939,
        "historyId": "2302",
        "internalDate": "1617303299000",
    }
    get_message2 = {
        "id": "1788ebe4ecbab055",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service "
        "Account, Your usage report &quot;Book Usage by Country&quot; is now available to "
        "download. Download Completed Report",
        "payload": {
            "partId": "",
            "mimeType": "text/html",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {
                "size": 12313,
                "data": base64.urlsafe_b64encode(
                    f'<a href="{institution_report_url}">Download Completed Report</a>'.encode()
                ).decode(),
            },
        },
        "sizeEstimate": 17939,
        "historyId": "2302",
        "internalDate": "1617303299000",
    }
    get_message3 = {
        "id": "5621ayw3vjtag411",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service "
        "Account, Your usage report &quot;Book Usage by Country&quot; is now available to "
        "download. Download Completed Report",
        "payload": {
            "partId": "",
            "mimeType": "text/html",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {
                "size": 12313,
                "data": base64.urlsafe_b64encode(
                    f'<a href="{wrong_publisher_report_url}">Download Completed Report</a>'.encode()
                ).decode(),
            },
        },
        "sizeEstimate": 17939,
        "historyId": "2302",
        "internalDate": "1617303299000",
    }
    modify_message1 = {
        "id": "1788ec9e91f3de62",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["Label_1", "CATEGORY_PERSONAL", "INBOX"],
    }
    modify_message2 = {
        "id": "1788ebe4ecbab055",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["Label_1", "CATEGORY_PERSONAL", "INBOX"],
    }
    http_mock_sequence = [
        ({"status": "200"}, json.dumps(list_messages1)),
        ({"status": "200"}, json.dumps(list_messages2)),
        ({"status": "200"}, json.dumps(get_message1)),
        ({"status": "200"}, json.dumps(get_message2)),
        ({"status": "200"}, json.dumps(get_message3)),
        ({"status": "200"}, json.dumps(list_labels)),
        ({"status": "200"}, json.dumps(modify_message1)),
        ({"status": "200"}, json.dumps(modify_message2)),
    ]

    return http_mock_sequence
