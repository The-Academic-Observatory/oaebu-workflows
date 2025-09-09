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
from oaebu_workflows.muse_telescope.muse_telescope import DATE_PARTITION_FIELD, muse_row_transform
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
        self.assertTrue(all(DATE_PARTITION_FIELD in r for r in result))
        # Feb 2024 last day should be 2024-02-29
        self.assertEqual(result[0][DATE_PARTITION_FIELD], str(pendulum.date(2024, 2, 29)))

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
