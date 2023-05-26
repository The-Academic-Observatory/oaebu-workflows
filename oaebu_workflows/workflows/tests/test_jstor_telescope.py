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
from airflow.utils.state import State
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.jstor_telescope import JstorRelease, JstorTelescope, get_label_id, get_release_date
from observatory.platform.observatory_environment import ObservatoryEnvironment, ObservatoryTestCase, find_free_port
from observatory.platform.observatory_config import Workflow
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.bigquery import bq_table_id
from observatory.platform.api import get_dataset_releases


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
        self.publisher_id = "anupress"

        self.release_date = pendulum.parse("20220701").end_of("month")
        self.country_report = {
            "path": test_fixtures_folder("jstor", "country_20220801.tsv"),
            "url": "https://www.jstor.org/admin/reports/download/249192019",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_{self.publisher_id}_PUBBCU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
            "download_hash": "9330cc71f8228838ac84abb33cedb3b8",
            "transform_hash": "5a72fe64",
            "table_rows": 10,
        }
        self.institution_report = {
            "path": test_fixtures_folder("jstor", "institution_20220801.tsv"),
            "url": "https://www.jstor.org/admin/reports/download/129518301",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_{self.publisher_id}_PUBBIU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
            "download_hash": "1c78c316766a3f7306d6c19440250484",
            "transform_hash": "f339ea81",
            "table_rows": 3,
        }
        self.wrong_publisher_report = {
            "path": test_fixtures_folder("jstor", "institution_20220801.tsv"),  # has to be valid path, but is not used
            "url": "https://www.jstor.org/admin/reports/download/12345",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_publisher_PUBBIU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
        }

    def test_dag_structure(self):
        """Test that the Jstor DAG has the correct structure."""
        dag = JstorTelescope(
            "jstor", cloud_workspace=self.fake_cloud_workspace, publisher_id=self.publisher_id
        ).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["list_reports"],
                "list_reports": ["download_reports"],
                "download_reports": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Jstor DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="jstor_test_telescope",
                    name="My JSTOR Workflow",
                    class_name="oaebu_workflows.workflows.jstor_telescope.JstorTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publisher_id=self.publisher_id),
                )
            ],
        )
        with env.create():
            self.assert_dag_load_from_config("jstor_test_telescope")

    @patch("oaebu_workflows.workflows.jstor_telescope.build")
    @patch("oaebu_workflows.workflows.jstor_telescope.Credentials")
    def test_telescope(self, mock_account_credentials, mock_build):
        """Test the Jstor telescope end to end."""

        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(
            create_http_mock_sequence(
                self.country_report["url"], self.institution_report["url"], self.wrong_publisher_report["url"]
            )
        )
        mock_build.return_value = build("gmail", "v1", http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = JstorTelescope(
            dag_id="jstor_test_telescope",
            cloud_workspace=env.cloud_workspace,
            publisher_id=self.publisher_id,
            bq_dataset_id=dataset_id,
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                # add gmail connection
                conn = Connection(
                    conn_id="gmail_api",
                    uri="google-cloud-platform://?token=123&refresh_token=123"
                    "&client_id=123.apps.googleusercontent.com&client_secret=123",
                )
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test list releases task with files available
                with httpretty.enabled():
                    for report in [self.country_report, self.institution_report, self.wrong_publisher_report]:
                        self.setup_mock_file_download(
                            report["url"], report["path"], headers=report["headers"], method=httpretty.HEAD
                        )
                    ti = env.run_task(telescope.list_reports.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
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
                    self.assertEqual(ti.state, State.SUCCESS)

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
                release = JstorRelease(
                    dag_id=telescope.dag_id,
                    run_id=env.dag_run.run_id,
                    data_interval_start=pendulum.parse(release_date).start_of("month"),
                    data_interval_end=pendulum.parse(release_date).add(days=1).start_of("month"),
                    partition_date=pendulum.parse(release_date),
                    reports_info=reports_info,
                )

                self.assertTrue(os.path.exists(release.download_country_path))
                self.assertTrue(os.path.exists(release.download_institution_path))
                self.assert_file_integrity(release.download_country_path, self.country_report["download_hash"], "md5")
                self.assert_file_integrity(
                    release.download_institution_path, self.institution_report["download_hash"], "md5"
                )
                # Test that file uploaded
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_country_path),
                    release.download_country_path,
                )
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_institution_path),
                    release.download_institution_path,
                )

                # Test that file transformed
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertTrue(os.path.exists(release.transform_country_path))
                self.assertTrue(os.path.exists(release.transform_institution_path))
                self.assert_file_integrity(
                    release.transform_country_path, self.country_report["transform_hash"], "gzip_crc"
                )
                self.assert_file_integrity(
                    release.transform_institution_path, self.institution_report["transform_hash"], "gzip_crc"
                )

                # Test that transformed file uploaded
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_country_path),
                    release.transform_country_path,
                )
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_institution_path),
                    release.transform_institution_path,
                )

                # Test that data loaded into BigQuery
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                country_table_id = bq_table_id(
                    telescope.cloud_workspace.project_id, telescope.bq_dataset_id, telescope.bq_country_table_name
                )
                institution_table_id = bq_table_id(
                    telescope.cloud_workspace.project_id,
                    telescope.bq_dataset_id,
                    telescope.bq_institution_table_name,
                )
                self.assert_table_integrity(country_table_id, self.country_report["table_rows"])
                self.assert_table_integrity(institution_table_id, self.institution_report["table_rows"])

                # Add_dataset_release_task
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test that all telescope data deleted
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(release.workflow_folder)

    def test_get_label_id(self):
        """Test getting label id both when label already exists and does not exist yet."""
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
        incorrect"""
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
            start_date, end_date = get_release_date(reports[0]["file"])
            self.assertEqual(pendulum.parse(reports[0]["start"]), start_date)
            self.assertEqual(pendulum.parse(reports[0]["end"]), end_date)
            # Test new report fails
            with self.assertRaises(AirflowException):
                get_release_date(reports[1]["file"])
            # Test old report is successful
            start_date, end_date = get_release_date(reports[2]["file"])
            self.assertEqual(pendulum.parse(reports[2]["start"]).start_of("month").start_of("day"), start_date)
            self.assertEqual(pendulum.parse(reports[2]["end"]).end_of("month").start_of("day"), end_date)
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
