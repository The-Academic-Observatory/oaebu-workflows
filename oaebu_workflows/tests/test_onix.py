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
import unittest
from unittest.mock import patch

from oaebu_workflows.onix import (
    onix_collapse_subjects,
    onix_create_personname_fields,
    onix_parser_download,
    onix_parser_execute,
    elevate_related_products,
)
from oaebu_workflows.config import test_fixtures_folder
from observatory.platform.observatory_environment import ObservatoryTestCase



class TestOnixFunctions(ObservatoryTestCase):
    """Tests for the ONIX telescope"""

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
            shutil.copy(test_fixtures_folder("onix", "20210330_CURTINPRESS_ONIX.xml"), input_dir)
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

    def test_onix_create_personname_fields(self):
        """Tests the function that creates the personname field"""
        input_onix = [
            {
                "Contributors": [
                    {"PersonName": "John Doe", "PersonNameInverted": None, "KeyNames": None, "NamesBeforeKey": None}
                ]
            },
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": None, "KeyNames": "Doe", "NamesBeforeKey": "John"}
                ]
            },
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": "Doe, John", "KeyNames": "Doe", "NamesBeforeKey": None}
                ]
            },
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": None, "KeyNames": None, "NamesBeforeKey": None}
                ]
            },
            {"empty": "empty"},
        ]
        expected_out = [
            {
                "Contributors": [
                    {
                        "PersonName": "John Doe",
                        "PersonNameInverted": None,
                        "KeyNames": None,
                        "NamesBeforeKey": None,
                    }
                ]
            },
            {
                "Contributors": [
                    {
                        "PersonName": "John Doe",
                        "PersonNameInverted": "Doe, John",
                        "KeyNames": "Doe",
                        "NamesBeforeKey": "John",
                    }
                ]
            },
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": "Doe, John", "KeyNames": "Doe", "NamesBeforeKey": None}
                ]
            },
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": None, "KeyNames": None, "NamesBeforeKey": None}
                ]
            },
            {"empty": "empty"},
        ]
        output_onix = onix_create_personname_fields(input_onix)
        self.assertEqual(len(output_onix), len(expected_out))
        for actual, expected in zip(output_onix, expected_out):
            self.assertDictEqual(actual, expected)


class TestElevateRelatedProducts(unittest.TestCase):
    def test_related_product_elevation(self):
        """Generic test use case"""
        product = {
            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1234567890"},
            "RelatedMaterial": {
                "RelatedProduct": {"ProductIdentifier": [{"ProductIDType": "15", "IDValue": "0987654321"}]}
            },
            "RecordReference": "ABC123",
        }
        expected_result = [
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1234567890"},
                "RelatedMaterial": {
                    "RelatedProduct": {"ProductIdentifier": [{"ProductIDType": "15", "IDValue": "0987654321"}]}
                },
                "RecordReference": "ABC123",
            },
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "0987654321"},
                "RelatedMaterial": {
                    "RelatedProduct": {"ProductIdentifier": [{"ProductIDType": "15", "IDValue": "1234567890"}]}
                },
                "RecordReference": "ABC123_0987654321",
            },
        ]

        result = elevate_related_products(product)
        self.assertEqual(result, expected_result)

    def test_no_related_material(self):
        """Test with a product that has no RelatedMaterial"""
        product = {"ProductIdentifier": {"ProductIDType": "15", "IDValue": "1234567890"}, "RecordReference": "ABC123"}
        expected_result = [
            {"ProductIdentifier": {"ProductIDType": "15", "IDValue": "1234567890"}, "RecordReference": "ABC123"}
        ]

        result = elevate_related_products(product)
        self.assertEqual(result, expected_result)

    def test_related_material_no_isbn(self):
        """Test with a product that has RelatedMaterial without isbns"""
        product = {
            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "0987654321"},
            "RelatedMaterial": {
                "RelatedProduct": {"ProductIdentifier": [{"ProductIDType": "10", "IDValue": "ABC123"}]}
            },
            "RecordReference": "XYZ",
        }
        expected_result = [
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "0987654321"},
                "RelatedMaterial": {
                    "RelatedProduct": {"ProductIdentifier": [{"ProductIDType": "10", "IDValue": "ABC123"}]}
                },
                "RecordReference": "XYZ",
            }
        ]

        result = elevate_related_products(product)
        self.assertEqual(result, expected_result)

    def test_no_isbn_product_identifier(self):
        # Test with a product that has a ProductIdentifier that is not an ISBN. No changes should occur.
        product = {
            "ProductIdentifier": {"ProductIDType": "10", "IDValue": "ABC123"},
            "RelatedMaterial": {
                "RelatedProduct": {"ProductIdentifier": [{"ProductIDType": "15", "IDValue": "0987654321"}]}
            },
            "RecordReference": "XYZ",
        }
        expected_result = [
            {
                "ProductIdentifier": {"ProductIDType": "10", "IDValue": "ABC123"},
                "RelatedMaterial": {
                    "RelatedProduct": {"ProductIdentifier": [{"ProductIDType": "15", "IDValue": "0987654321"}]}
                },
                "RecordReference": "XYZ",
            },
        ]

        result = elevate_related_products(product)
        self.assertEqual(result, expected_result)
