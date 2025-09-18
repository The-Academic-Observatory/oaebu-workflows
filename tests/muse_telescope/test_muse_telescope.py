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

import base64
import json
import os
from unittest.mock import Mock, patch

import httpretty
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from oaebu_workflows.config import test_fixtures_folder, module_file_path
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.muse_telescope.muse_telescope import (
    add_label,
    create_gmail_service,
    download_attachment,
    get_message_attachement_ids,
    get_messages,
    get_release_date_from_report,
    muse_data_transform,
    muse_row_transform,
    read_gzipped_report,
)
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_upload_files
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase, load_and_parse_json

import unittest
import logging
from unittest.mock import patch
import pendulum


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

    @patch('oaebu_workflows.muse_telescope.muse_telescope.muse_row_transform')
    def test_muse_data_transform(self, mock_muse_row_transform):
        mock_muse_row_transform.side_effect = lambda row: [row]  # Return the row in a list
        data = [{'id': 1}, {'id': 2}]

        result = muse_data_transform(data)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, data)
        self.assertEqual(mock_muse_row_transform.call_count, 2)


class TestReadGzippedReport(unittest.TestCase):

    def test_read_gzipped_report(self):
        fixture_path = os.path.join(test_fixtures_folder(workflow_module="muse_telescope"), "muse_report.csv.gz")

        data = read_gzipped_report(fixture_path)

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['year'], '2024')
        self.assertEqual(data[0]['month'], '2')
        self.assertEqual(data[0]['isbns'], '1234567890123')
        self.assertEqual(data[0]['title'], 'Sample Book 1')


class TestGetReleaseDateFromReport(unittest.TestCase):

    @patch('oaebu_workflows.muse_telescope.muse_telescope.read_gzipped_report')
    def test_get_release_date_from_report_success(self, mock_read_gzipped_report):
        mock_read_gzipped_report.return_value = [
            {'year': '2024', 'month': '2'},
            {'year': '2024', 'month': '2'}
        ]

        release_date = get_release_date_from_report('dummy_path')

        self.assertEqual(release_date, pendulum.datetime(2024, 2, 1).end_of('month'))

    @patch('oaebu_workflows.muse_telescope.muse_telescope.read_gzipped_report')
    def test_get_release_date_from_report_no_date(self, mock_read_gzipped_report):
        mock_read_gzipped_report.return_value = [{'title': 'book'}]

        release_date = get_release_date_from_report('dummy_path')

        self.assertIsNone(release_date)

    @patch('oaebu_workflows.muse_telescope.muse_telescope.read_gzipped_report')
    def test_get_release_date_from_report_inconsistent_date(self, mock_read_gzipped_report):
        mock_read_gzipped_report.return_value = [
            {'year': '2024', 'month': '2'},
            {'year': '2024', 'month': '3'}
        ]

        with self.assertRaises(RuntimeError):
            get_release_date_from_report('dummy_path')


class TestGmailFunctions(unittest.TestCase):

    def setUp(self):
        self.mock_service = Mock()

    def test_create_gmail_service(self):
        with patch('oaebu_workflows.muse_telescope.muse_telescope.BaseHook') as mock_hook, \
             patch('oaebu_workflows.muse_telescope.muse_telescope.Credentials') as mock_creds, \
             patch('oaebu_workflows.muse_telescope.muse_telescope.build') as mock_build:
            mock_conn = Mock()
            mock_conn.extra_dejson = {}
            mock_hook.get_connection.return_value = mock_conn
            mock_creds.from_authorized_user_info.return_value = 'dummy_creds'
            mock_build.return_value = self.mock_service

            service = create_gmail_service()

            self.assertEqual(service, self.mock_service)
            mock_hook.get_connection.assert_called_once_with("gmail_api")
            mock_creds.from_authorized_user_info.assert_called_once()
            mock_build.assert_called_once_with("gmail", "v1", credentials='dummy_creds', cache_discovery=False)

    def test_download_attachment(self):
        self.mock_service.users.return_value.messages.return_value.attachments.return_value.get.return_value.execute.return_value = {
            'data': base64.urlsafe_b64encode(b'test_data').decode('utf-8')
        }
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            download_attachment(self.mock_service, 'msg_id', 'att_id', 'dummy_path')

            mock_file.assert_called_once_with('dummy_path', 'wb')
            mock_file().write.assert_called_once_with(b'test_data')

    def test_get_messages(self):
        self.mock_service.users.return_value.messages.return_value.list.return_value.execute.side_effect = [
            {'messages': [{'id': 1}], 'nextPageToken': 'token'},
            {'messages': [{'id': 2}]}
        ]

        messages = get_messages(self.mock_service, {}) 

        self.assertEqual(messages, [{'id': 1}, {'id': 2}])

    def test_get_message_attachement_ids(self):
        self.mock_service.users.return_value.messages.return_value.get.return_value.execute.return_value = {
            'payload': {
                'parts': [
                    {'filename': 'file1.csv', 'body': {'attachmentId': 'att_id_1'}},
                    {'filename': '', 'body': {}}, # Not an attachment
                    {'filename': 'file2.csv', 'body': {'attachmentId': 'att_id_2'}}
                ]
            }
        }

        ids = get_message_attachement_ids(self.mock_service, 'msg_id')

        self.assertEqual(ids, ['att_id_1', 'att_id_2'])

    def test_add_label_success(self):
        self.mock_service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {'id': 'msg_id'}

        success = add_label(self.mock_service, 'msg_id', 'label_name')

        self.assertTrue(success)

    def test_add_label_failure(self):
        self.mock_service.users.return_value.messages.return_value.modify.return_value.execute.return_value = {}

        success = add_label(self.mock_service, 'msg_id', 'label_name')

        self.assertFalse(success)
