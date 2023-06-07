# Copyright 2023 Curtin University
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

import os
import shutil
from tempfile import TemporaryDirectory
import json
from unittest.mock import patch

from oaebu_workflows.onix import (
    onix_collapse_subjects,
    onix_create_personname_field,
    onix_parser_download,
    onix_parser_execute,
)
from oaebu_workflows.config import test_fixtures_folder
from observatory.platform.observatory_environment import ObservatoryTestCase


class TestOnixFunctions(ObservatoryTestCase):
    """Tests for the ONIX telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestOnixFunctions, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.onix_test_path = test_fixtures_folder("onix", "20210330_CURTINPRESS_ONIX.xml")

    def test_onix_parser_download_execute(self):
        """Tests the onix_parser_download and onix_parser_execute functions"""
        with TemporaryDirectory() as tempdir:
            ### Test parser download - fails ###
            with patch("oaebu_workflows.onix.download_file") as mock_download:
                mock_download.return_value = (False, "")
                success, parser_path = onix_parser_download(download_dir=tempdir)
                self.assertEqual(success, False)
                mock_download.return_value = (True, "/this/path/does/not/exist")
                success, parser_path = onix_parser_download(download_dir=tempdir)
                self.assertEqual(success, False)

            ### Test parser download - succeeds ###
            success, parser_path = onix_parser_download(download_dir=tempdir)
            self.assertIn(tempdir, parser_path)
            self.assertEqual(success, True)

            ### Test parser_execute: onix.xml does not exist in input folder ###
            input_dir = os.path.join(tempdir, "input")
            output_dir = os.path.join(tempdir, "output")
            os.mkdir(input_dir)
            os.mkdir(output_dir)
            success = onix_parser_execute(parser_path=parser_path, input_dir=input_dir, output_dir=output_dir)
            self.assertEqual(success, False)

            ### Test parser_execute: nonzero returncode ###
            shutil.copy(self.onix_test_path, input_dir)
            with patch("oaebu_workflows.onix.wait_for_process") as mock_wfp:
                mock_wfp.return_value = ("stdout", "stderr")
                with patch("oaebu_workflows.onix.subprocess.Popen") as mock_popen:
                    mock_popen.returncode = 1
                    success = onix_parser_execute(parser_path=parser_path, input_dir=input_dir, output_dir=output_dir)
            self.assertEqual(success, False)

            ### Test Case: Successful run ###
            success = onix_parser_execute(parser_path=parser_path, input_dir=input_dir, output_dir=output_dir)
            output_file = os.path.join(output_dir, "full.jsonl")
            self.assertEqual(success, True)
            self.assertTrue(os.path.isfile(output_file))
            self.assert_file_integrity(output_file, "84d46e2942df615f18d270e18e0ebb26", "md5")

    def test_onix_collapse_subjects(self):
        """Tests the thoth_collapse_subjects function"""
        test_subjects_input = os.path.join(test_fixtures_folder("onix"), "test_subjects_input.json")
        test_subjects_expected = os.path.join(test_fixtures_folder("onix"), "test_subjects_expected.json")
        with open(test_subjects_input, "r") as f:
            onix = json.load(f)
        actual_onix = onix_collapse_subjects(onix)
        with open(test_subjects_expected, "r") as f:
            expected_onix = json.load(f)

        self.assertEqual(len(actual_onix), len(expected_onix))
        self.assertEqual(json.dumps(actual_onix, sort_keys=True), json.dumps(expected_onix, sort_keys=True))

    def test_onix_create_personname_field(self):
        """Tests the function that creates the personname field"""
        input_onix = [
            {"Contributors": [{"PersonName": "John Doe", "KeyNames": None, "NamesBeforeKey": None}]},
            {"Contributors": [{"PersonName": None, "KeyNames": "Doe", "NamesBeforeKey": "John"}]},
            {"Contributors": [{"PersonName": None, "KeyNames": "Doe", "NamesBeforeKey": None}]},
            {"Contributors": [{"PersonName": None, "KeyNames": None, "NamesBeforeKey": None}]},
            {"empty": "empty"},
        ]
        expected_out = [
            {"Contributors": [{"PersonName": "John Doe", "KeyNames": None, "NamesBeforeKey": None}]},
            {"Contributors": [{"PersonName": "John Doe", "KeyNames": "Doe", "NamesBeforeKey": "John"}]},
            {"Contributors": [{"PersonName": None, "KeyNames": "Doe", "NamesBeforeKey": None}]},
            {"Contributors": [{"PersonName": None, "KeyNames": None, "NamesBeforeKey": None}]},
            {"empty": "empty"},
        ]
        output_onix = onix_create_personname_field(input_onix)
        self.assertEqual(len(output_onix), len(expected_out))
        for actual, expected in zip(output_onix, expected_out):
            self.assertDictEqual(actual, expected)
