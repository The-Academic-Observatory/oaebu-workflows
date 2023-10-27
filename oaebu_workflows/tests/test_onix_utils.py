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

from oaebu_workflows.onix_utils import (
    collapse_subjects,
    create_personname_fields,
    onix_parser_download,
    onix_parser_execute,
    elevate_product_identifiers,
    normalise_related_products,
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
            with patch("oaebu_workflows.onix_utils.download_file") as mock_download:
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
            shutil.copy(test_fixtures_folder("onix_telescope", "20210330_CURTINPRESS_ONIX.xml"), input_dir)
            with patch("oaebu_workflows.onix_utils.wait_for_process") as mock_wfp:
                mock_wfp.return_value = ("stdout", "stderr")
                with patch("oaebu_workflows.onix_utils.subprocess.Popen") as mock_popen:
                    mock_popen.returncode = 1
                    success = onix_parser_execute(parser_path=parser_path, input_dir=input_dir, output_dir=output_dir)
            self.assertEqual(success, False)

            ### Test Case: Successful run ###
            success = onix_parser_execute(parser_path=parser_path, input_dir=input_dir, output_dir=output_dir)
            output_file = os.path.join(output_dir, "full.jsonl")
            self.assertEqual(success, True)
            self.assertTrue(os.path.isfile(output_file))
            self.assert_file_integrity(output_file, "84d46e2942df615f18d270e18e0ebb26", "md5")

    def test_collapse_subjects(self):
        """Tests the thoth_collapse_subjects function"""
        test_subjects_input = os.path.join(test_fixtures_folder("onix_utils"), "test_subjects_input.json")
        test_subjects_expected = os.path.join(test_fixtures_folder("onix_utils"), "test_subjects_expected.json")
        with open(test_subjects_input, "r") as f:
            onix = json.load(f)
        actual_onix = collapse_subjects(onix)
        with open(test_subjects_expected, "r") as f:
            expected_onix = json.load(f)

        self.assertEqual(len(actual_onix), len(expected_onix))
        self.assertEqual(json.dumps(actual_onix, sort_keys=True), json.dumps(expected_onix, sort_keys=True))


class TestCreatePersonnameFields(unittest.TestCase):
    def test_populate_personname_field(self):
        """Given a list of ONIX products with Contributors that have both NamesBeforeKey and KeyNames fields,
        the function should populate the PersonName field with the concatenation of NamesBeforeKey and KeyNames,
        and also populate the PersonNameInverted field with the concatenation of KeyNames and NamesBeforeKey"""
        input_onix = [
            {
                "DescriptiveDetail": {
                    "Contributor": [
                        {"PersonName": None, "PersonNameInverted": None, "KeyNames": "Doe", "NamesBeforeKey": "John"}
                    ]
                }
            },
            {
                "DescriptiveDetail": {
                    "Contributor": [
                        {"PersonName": None, "PersonNameInverted": None, "KeyNames": "Smith", "NamesBeforeKey": "Jane"}
                    ]
                }
            },
        ]
        expected_output = [
            {
                "DescriptiveDetail": {
                    "Contributor": [
                        {
                            "PersonName": "John Doe",
                            "PersonNameInverted": "Doe, John",
                            "KeyNames": "Doe",
                            "NamesBeforeKey": "John",
                        }
                    ]
                }
            },
            {
                "DescriptiveDetail": {
                    "Contributor": [
                        {
                            "PersonName": "Jane Smith",
                            "PersonNameInverted": "Smith, Jane",
                            "KeyNames": "Smith",
                            "NamesBeforeKey": "Jane",
                        }
                    ]
                }
            },
        ]
        output_onix = create_personname_fields(input_onix)
        self.assertEqual(output_onix, expected_output)

    def test_no_modification(self):
        """Should not modify when there is not enough information to populate PersonName or PersonNameInverted"""
        input_onix = [
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": None, "KeyNames": None, "NamesBeforeKey": "John"}
                ]
            },
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": None, "KeyNames": "Doe", "NamesBeforeKey": None}
                ]
            },
        ]
        output_onix = create_personname_fields(input_onix)
        self.assertEqual(output_onix, input_onix)  # Should not change input

    def test_prepopulated_personnames(self):
        """Should not modify when PersonName fields are already populated"""
        input_onix = [
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
                    {
                        "PersonName": "Wrong Name",
                        "PersonNameInverted": "Doe, Jane",
                        "KeyNames": "Doe",
                        "NamesBeforeKey": "Jane",
                    }
                ]
            },
        ]
        output_onix = create_personname_fields(input_onix)
        self.assertEqual(output_onix, input_onix)  # Should not change input


class TestNormaliseRelatedProducts(unittest.TestCase):
    """Contains tests for normalise_related_products and elevate_product_identifiers"""

    def test_normalise_related_products(self):
        """Should correctly elevate <ProductIdentifier> elements to <RelatedProduct> elements"""
        onix_products = [
            {
                "ProductID": "123",
                "RelatedMaterial": {
                    "RelatedProduct": {
                        "ProductRelationCode": "01",
                        "ProductIdentifier": [
                            {"ProductIDType": "01", "IDValue": "456"},
                            {"ProductIDType": "02", "IDValue": "789"},
                        ],
                    }
                },
            },
            {
                "ProductID": "456",
                "RelatedMaterial": {
                    "RelatedProduct": {
                        "ProductRelationCode": "01",
                        "ProductIdentifier": [
                            {"ProductIDType": "01", "IDValue": "123"},
                            {"ProductIDType": "02", "IDValue": "789"},
                        ],
                    }
                },
            },
            {
                "ProductID": "789",
                "RelatedMaterial": {
                    "RelatedProduct": {
                        "ProductRelationCode": "02",
                        "ProductIdentifier": {"ProductIDType": "03", "IDValue": "ABC"},
                    }
                },
            },
        ]

        expected_result = [
            {
                "ProductID": "123",
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {"ProductRelationCode": "01", "ProductIdentifier": [{"ProductIDType": "01", "IDValue": "456"}]},
                        {"ProductRelationCode": "01", "ProductIdentifier": [{"ProductIDType": "02", "IDValue": "789"}]},
                    ]
                },
            },
            {
                "ProductID": "456",
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {"ProductRelationCode": "01", "ProductIdentifier": [{"ProductIDType": "01", "IDValue": "123"}]},
                        {"ProductRelationCode": "01", "ProductIdentifier": [{"ProductIDType": "02", "IDValue": "789"}]},
                    ]
                },
            },
            {
                "ProductID": "789",
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {"ProductRelationCode": "02", "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "ABC"}]}
                    ]
                },
            },
        ]

        result = normalise_related_products(onix_products)
        self.assertEqual(result, expected_result)

    def test_elevate_product_identifiers(self):
        """Should return a list of <RelatedProduct> structured elements. There should be one elements for each unique
        <ProductIdentifier><IDValue>. If there are multiple <ProductIdentifier><IDValue> elements with different
        <ProductIDType> then these are grouped in the return"""
        related_product = {
            "ProductRelationCode": "ABC",
            "ProductIdentifier": [
                {"ProductIDType": "06", "IDValue": "12345"},
                {"ProductIDType": "15", "IDValue": "12345"},
                {"ProductIDType": "06", "IDValue": "67890"},
            ],
        }
        expected_result = [
            {
                "ProductRelationCode": "ABC",
                "ProductIdentifier": [
                    {"ProductIDType": "06", "IDValue": "12345"},
                    {"ProductIDType": "15", "IDValue": "12345"},
                ],
            },
            {"ProductRelationCode": "ABC", "ProductIdentifier": [{"ProductIDType": "06", "IDValue": "67890"}]},
        ]
        result = elevate_product_identifiers(related_product)
        self.assertEqual(result, expected_result)

    def test_duplicate_entries(self):
        """Should return a list with a single element when given duplicate entries"""
        related_product = {
            "ProductRelationCode": "ABC",
            "ProductIdentifier": [
                {"ProductIDType": "06", "IDValue": "12345"},
                {"ProductIDType": "06", "IDValue": "12345"},
            ],
        }
        expected_result = [
            {"ProductRelationCode": "ABC", "ProductIdentifier": [{"ProductIDType": "06", "IDValue": "12345"}]}
        ]
        result = elevate_product_identifiers(related_product)
        self.assertEqual(result, expected_result)


class TestElevateRelatedProducts(unittest.TestCase):
    # Should return the same input list if no related products have an ISBN
    def test_no_isbn_related_products(self):
        expected_products = [
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1234567890"},
                "RelatedMaterial": {
                    "RelatedProduct": {
                        "ProductRelationCode": "01",
                        "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "9876543210"}],
                    }
                },
            }
        ]

        result = elevate_related_products(expected_products)
        self.assertEqual(result, expected_products)  # Should not change

    # Should return the same input list if no related products are present
    def test_no_related_products(self):
        onix_products = [
            {"ProductIdentifier": {"ProductIDType": "15", "IDValue": "1234567890"}, "RelatedMaterial": {}},
        ]

        result = elevate_related_products(onix_products)
        self.assertEqual(result, onix_products)  # Should not change

    def test_no_parent_isbn(self):
        """Should return the same input list if the parent product has no ISBN"""
        onix_products = [
            {
                "ProductIdentifier": {"ProductIDType": "03", "IDValue": "1234567890"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "0987654321"},
                        }
                    ]
                },
            }
        ]

        result = elevate_related_products(onix_products)
        self.assertEqual(result, onix_products)  # Should not change

    def test_relation_code(self):
        """Shuold not elevate related products if the relation code is no '06'"""
        onix_products = [
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1234567890"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "03",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "0987654321"},
                        }
                    ]
                },
            }
        ]

        result = elevate_related_products(onix_products)
        self.assertEqual(result, onix_products)  # Should not change

    def test_duplicate_ids(self):
        """Should not elevate related products if the ID is a duplicate of an existing parent product"""
        onix_products = [
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "2"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1"},
                        }
                    ]
                },
            },
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1"},
                "RelatedMaterial": {"RelatedProduct": []},
            },
        ]

        result = elevate_related_products(onix_products)
        self.assertEqual(result, onix_products)  # Should not change

    def test_related_product_elevation(self):
        """Should elevate ISBN related products to the product level when the parent product has an ISBN"""
        onix_products = [
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1.1"},
                        },
                    ]
                },
                "RecordReference": "product1",
            },
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "2"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "2.1"},
                        }
                    ]
                },
                "RecordReference": "product2",
            },
        ]

        expected_result = [
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1.1"},
                        },
                    ]
                },
                "RecordReference": "product1",
            },
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "2"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "2.1"},
                        }
                    ]
                },
                "RecordReference": "product2",
            },
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1.1"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "1"},
                        },
                    ]
                },
                "RecordReference": "product1_1.1",
            },
            {
                "ProductIdentifier": {"ProductIDType": "15", "IDValue": "2.1"},
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "06",
                            "ProductIdentifier": {"ProductIDType": "15", "IDValue": "2"},
                        }
                    ]
                },
                "RecordReference": "product2_2.1",
            },
        ]
        result = elevate_related_products(onix_products)
        self.assertEqual(result, expected_result)
