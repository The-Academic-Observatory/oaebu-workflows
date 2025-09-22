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

# Author: Keegan Smith

import base64
import json
import os
import gzip
import unittest
from unittest.mock import Mock, patch
from tempfile import NamedTemporaryFile

import pendulum
from airflow.models.connection import Connection
from airflow.utils.state import State
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from oaebu_workflows.config import test_fixtures_folder, module_file_path
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.muse_telescope.muse_telescope import (
    add_label_to_message,
    create_dag,
    create_gmail_service,
    download_attachment,
    get_message_attachement_ids,
    get_messages,
    get_release_date_from_report,
    muse_data_transform,
    muse_row_transform,
    read_gzipped_report,
    MuseRelease,
)
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_table_id
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase, load_and_parse_json
from observatory_platform.files import load_jsonl


def dummy_gmail_connection() -> Connection:
    return Connection(
        conn_id="gmail_api",
        uri="google-cloud-platform://?token=123&refresh_token=123"
        "&client_id=123.apps.googleusercontent.com&client_secret=123",
    )


class TestTelescopeSetup(SandboxTestCase):
    def __init__(self, *args, **kwargs):
        super(TestTelescopeSetup, self).__init__(*args, **kwargs)

    def test_dag_structure(self):
        """Test that the Muse DAG has the correct structure."""
        env = SandboxEnvironment()
        with env.create():
            env.add_connection(dummy_gmail_connection())
            dag = create_dag(
                dag_id="muse_test",
                cloud_workspace=self.fake_cloud_workspace,
                publisher_subject_line="test_subject",
            )
            self.assert_dag_structure(
                {
                    "check_dependencies": ["create_releases"],
                    "create_releases": [
                        "process_release.transform",
                        "process_release.bq_load",
                        "process_release.add_new_dataset_releases",
                        "process_release.add_label",
                        "process_release.cleanup_workflow",
                    ],
                    "process_release.transform": ["process_release.bq_load"],
                    "process_release.bq_load": ["process_release.add_new_dataset_releases"],
                    "process_release.add_new_dataset_releases": ["process_release.add_label"],
                    "process_release.add_label": ["process_release.cleanup_workflow"],
                    "process_release.cleanup_workflow": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that the Muse DAG can be loaded from a DAG bag."""
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id="muse_test_telescope",
                    name="My MUSE Workflow",
                    class_name="oaebu_workflows.muse_telescope.muse_telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publisher_subject_line="test_subject"),
                )
            ],
        )

        with env.create():
            env.add_connection(dummy_gmail_connection())
            dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
            self.assert_dag_load_from_config("muse_test_telescope", dag_file)


class TestMuseTelescope(SandboxTestCase):
    """Tests for the Muse telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests."""
        super(TestMuseTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.report_file = NamedTemporaryFile(delete=False, suffix=".csv.gz").name
        self.release_date = pendulum.parse("20240201").end_of("month")

        # Gzip the plain .csv content
        csv_file = os.path.join(test_fixtures_folder("muse_telescope"), "e2e_input.csv")
        with open(csv_file, "rb") as f:
            with gzip.open(self.report_file, "wb") as gzf:
                gzf.write(f.read())

    @patch("oaebu_workflows.muse_telescope.muse_telescope.build")
    @patch("oaebu_workflows.muse_telescope.muse_telescope.Credentials")
    def test_telescope(self, mock_account_credentials, mock_build):
        """Test the Muse telescope end to end."""

        mock_account_credentials.from_authorized_user_info.return_value = ""
        http = HttpMockSequence(muse_http_mock_sequence(self.report_file))
        mock_build.return_value = build("gmail", "v1", http=http, cache_discovery=False)

        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            # Add gmail connection
            env.add_connection(dummy_gmail_connection())

            # Setup Telescope
            logical_date = pendulum.datetime(year=2024, month=3, day=1)
            partner = partner_from_str("muse")
            dataset_id = env.add_dataset()
            partner.bq_dataset_id = dataset_id
            api_bq_dataset_id = env.add_dataset()
            dag_id = "muse_test_telescope"
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=env.cloud_workspace,
                publisher_subject_line="MUSE Counter 5 Report",
                data_partner=partner,
                api_bq_dataset_id=api_bq_dataset_id,
            )

            # Begin DAG run
            with env.create_dag_run(dag, logical_date=logical_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(ti.state, State.SUCCESS)

                # Test create_releases task
                ti = env.run_task("create_releases")
                self.assertEqual(ti.state, State.SUCCESS)

                release_dicts = ti.xcom_pull(task_ids="create_releases", include_prior_dates=False)
                self.assertEqual(len(release_dicts), 1)
                release = MuseRelease.from_dict(release_dicts[0])

                # Check that the file was "downloaded" and uploaded to GCS
                self.assert_blob_integrity(
                    env.download_bucket,
                    release.download_blob_name,
                    self.report_file,
                )

                # Test transform task
                ti = env.run_task("process_release.transform", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)

                # Check that transformed file was uploaded to GCS
                self.assert_blob_integrity(
                    env.transform_bucket,
                    release.transform_blob_name,
                    release.transform_path,
                )

                # Test bq_load task
                ti = env.run_task("process_release.bq_load", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)

                table_id = bq_table_id(
                    env.cloud_workspace.project_id,
                    partner.bq_dataset_id,
                    partner.bq_table_name,
                )
                self.assert_table_integrity(table_id, 6)
                expected_output_file = os.path.join(test_fixtures_folder("muse_telescope"), "e2e_output.json")
                expected_output = load_and_parse_json(expected_output_file, date_fields=["release_date"])
                self.assert_table_content(table_id, expected_content=expected_output, primary_key="isbn")

                # Test add_new_dataset_releases task
                api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=api_bq_dataset_id)
                self.assertEqual(len(api.get_dataset_releases(dag_id=dag_id, entity_id="muse")), 0)

                now = pendulum.now("UTC")
                with patch("oaebu_workflows.muse_telescope.muse_telescope.pendulum.now") as mock_now:
                    mock_now.return_value = now
                    ti = env.run_task("process_release.add_new_dataset_releases", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)

                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="muse")
                self.assertEqual(len(dataset_releases), 1)

                # Test add_label task
                ti = env.run_task("process_release.add_label", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test cleanup_workflow task
                workflow_folder_path = release.workflow_folder
                ti = env.run_task("process_release.cleanup_workflow", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(workflow_folder_path)


def muse_http_mock_sequence(report_path: str) -> list:
    """Create a list with mocked http responses"""

    list_messages = {
        "messages": [
            {"id": "12345", "threadId": "abcde"},
        ],
        "resultSizeEstimate": 1,
    }

    get_message = {
        "id": "12345",
        "threadId": "abcde",
        "labelIds": ["INBOX"],
        "payload": {
            "parts": [
                {"filename": "muse_report.csv.gz", "body": {"attachmentId": "att_id_1"}},
            ]
        },
    }

    with open(report_path, "rb") as f:
        attachment_data = base64.urlsafe_b64encode(f.read()).decode("utf-8")

    get_attachment = {"data": attachment_data}

    list_labels = {
        "labels": [
            {"id": "Label_1", "name": "INBOX", "type": "system"},
            {"id": "Label_2", "name": "IMPORTANT", "type": "system"},
            {"id": "Label_3", "name": "muse_processed", "type": "user"},
        ]
    }

    add_label_response = {"id": "12345", "labelIds": ["INBOX", "Label_3"]}
    http_mock_sequence = [
        ({"status": "200"}, json.dumps(list_messages)),
        ({"status": "200"}, json.dumps(get_message)),
        ({"status": "200"}, json.dumps(get_attachment)),
        ({"status": "200"}, json.dumps(list_labels)),
        ({"status": "200"}, json.dumps(add_label_response)),
    ]

    return http_mock_sequence


class TestMuseRowTransform(unittest.TestCase):
    def setUp(self):
        self.base_row = {
            "year": "2024",
            "month": "2",
            "isbns": "1234567890123,9876543210987,1234567890123",  # includes a duplicate
            "title": "Sample Book",
        }

    def test_date_partition_field_added(self):
        row = self.base_row.copy()
        result = muse_row_transform(row)
        self.assertTrue(all("release_date" in r for r in result))
        # Feb 2024 last day should be 2024-02-29
        self.assertEqual(result[0]["release_date"], str(pendulum.date(2024, 2, 29)))

    def test_valid_isbn_filtering(self):
        row = self.base_row.copy()
        row["isbns"] = "1234567890123,shortisbn,9876543210987"
        result = muse_row_transform(row)
        isbns = [r["isbn"] for r in result]
        self.assertIn("1234567890123", isbns)
        self.assertIn("9876543210987", isbns)
        self.assertNotIn("shortisbn", isbns)  # invalid

    def test_duplicates_are_removed(self):
        row = self.base_row.copy()
        row["isbns"] = "1234567890123,1234567890123,1234567890123"
        result = muse_row_transform(row)
        isbns = [r["isbn"] for r in result]
        self.assertEqual(len(isbns), 1)  # should deduplicate

    def test_multiple_valid_isbns_return_multiple_rows(self):
        row = self.base_row.copy()
        row["isbns"] = "1234567890123,9876543210987"
        result = muse_row_transform(row)
        self.assertEqual(len(result), 2)
        isbns = {r["isbn"] for r in result}
        self.assertEqual(isbns, {"1234567890123", "9876543210987"})

    def test_no_valid_isbns_returns_empty_list_and_warns(self):
        row = self.base_row.copy()
        row["isbns"] = "notvalid,alsoshort"
        result = muse_row_transform(row)
        self.assertEqual(result, [])


class TestMuseDataTransform(unittest.TestCase):

    @patch("oaebu_workflows.muse_telescope.muse_telescope.muse_row_transform")
    def test_muse_data_transform(self, mock_muse_row_transform):
        mock_muse_row_transform.side_effect = lambda row: [row]  # Return the row in a list
        data = [{"id": 1}, {"id": 2}]

        result = muse_data_transform(data)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, data)
        self.assertEqual(mock_muse_row_transform.call_count, 2)  # Should have been once for each list item


class TestReadGzippedReport(unittest.TestCase):
    def setUp(self):
        # Create a gzipped file for testing
        self.csv_data = (
            b"year,month,isbns,title\n2024,2,1234567890123,Sample Book 1\n2024,2,9876543210987,Sample Book 2"
        )
        tmp_file = NamedTemporaryFile(delete=False, suffix=".csv.gz")
        with gzip.open(tmp_file.name, "wb") as f:
            f.write(self.csv_data)
        self.report = tmp_file.name

    def test_read_gzipped_report(self):
        data = read_gzipped_report(self.report)

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["year"], "2024")
        self.assertEqual(data[0]["month"], "2")
        self.assertEqual(data[0]["isbns"], "1234567890123")
        self.assertEqual(data[0]["title"], "Sample Book 1")


class TestGetReleaseDateFromReport(unittest.TestCase):
    @patch("oaebu_workflows.muse_telescope.muse_telescope.read_gzipped_report")
    def test_get_release_date_from_report_success(self, mock_read_gzipped_report):
        mock_read_gzipped_report.return_value = [{"year": "2024", "month": "2"}, {"year": "2024", "month": "2"}]

        release_date = get_release_date_from_report("dummy_path")

        self.assertEqual(release_date, pendulum.datetime(2024, 2, 1).end_of("month").start_of("day"))

    @patch("oaebu_workflows.muse_telescope.muse_telescope.read_gzipped_report")
    def test_get_release_date_from_report_no_date(self, mock_read_gzipped_report):
        mock_read_gzipped_report.return_value = [{"title": "book"}]

        release_date = get_release_date_from_report("dummy_path")

        self.assertIsNone(release_date)

    @patch("oaebu_workflows.muse_telescope.muse_telescope.read_gzipped_report")
    def test_get_release_date_from_report_inconsistent_date(self, mock_read_gzipped_report):
        mock_read_gzipped_report.return_value = [{"year": "2024", "month": "2"}, {"year": "2024", "month": "3"}]

        with self.assertRaises(RuntimeError):
            get_release_date_from_report("dummy_path")


class TestGmailFunctions(unittest.TestCase):
    def setUp(self):
        self.mock_service = Mock()

    def test_create_gmail_service(self):
        with patch("oaebu_workflows.muse_telescope.muse_telescope.BaseHook") as mock_hook, patch(
            "oaebu_workflows.muse_telescope.muse_telescope.Credentials"
        ) as mock_creds, patch("oaebu_workflows.muse_telescope.muse_telescope.build") as mock_build:
            mock_conn = Mock()
            mock_conn.extra_dejson = {}
            mock_hook.get_connection.return_value = mock_conn
            mock_creds.from_authorized_user_info.return_value = "dummy_creds"
            mock_build.return_value = self.mock_service

            service = create_gmail_service()

            self.assertEqual(service, self.mock_service)
            mock_hook.get_connection.assert_called_once_with("gmail_api")
            mock_creds.from_authorized_user_info.assert_called_once()
            mock_build.assert_called_once_with("gmail", "v1", credentials="dummy_creds", cache_discovery=False)

    def test_download_attachment(self):
        self.mock_service.users.return_value.messages.return_value.attachments.return_value.get.return_value.execute.return_value = {
            "data": base64.urlsafe_b64encode(b"test_data").decode("utf-8")
        }
        with patch("builtins.open", unittest.mock.mock_open()) as mock_file:
            download_attachment(self.mock_service, "msg_id", "att_id", "dummy_path")

            mock_file.assert_called_once_with("dummy_path", "wb")
            mock_file().write.assert_called_once_with(b"test_data")

    def test_get_messages(self):
        self.mock_service.users.return_value.messages.return_value.list.return_value.execute.side_effect = [
            {"messages": [{"id": 1}], "nextPageToken": "token"},
            {"messages": [{"id": 2}]},
        ]

        messages = get_messages(self.mock_service, {})

        self.assertEqual(messages, [{"id": 1}, {"id": 2}])

    def test_get_message_attachement_ids(self):
        self.mock_service.users.return_value.messages.return_value.get.return_value.execute.return_value = {
            "payload": {
                "parts": [
                    {"filename": "file1.csv", "body": {"attachmentId": "att_id_1"}},
                    {"filename": "", "body": {}},  # Not an attachment
                    {"filename": "file2.csv", "body": {"attachmentId": "att_id_2"}},
                ]
            }
        }

        ids = get_message_attachement_ids(self.mock_service, "msg_id")

        self.assertEqual(ids, ["att_id_1", "att_id_2"])

    def test_add_label_success(self):
        self.mock_service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {
            "id": "msg_id",
            "labelIds": ["label_id"],
        }

        success = add_label_to_message(self.mock_service, "msg_id", "label_id")

        self.assertTrue(success)

    def test_add_label_failure(self):
        self.mock_service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {}

        success = add_label_to_message(self.mock_service, "msg_id", "label_id")

        self.assertFalse(success)
