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
from tempfile import TemporaryDirectory, NamedTemporaryFile
import json
import unittest
from unittest.mock import patch

from oaebu_workflows.onix_utils import (
    OnixTransformer,
    collapse_subjects,
    create_personname_fields,
    onix_parser_download,
    onix_parser_execute,
    elevate_product_identifiers,
    normalise_related_products,
    elevate_related_products,
    find_onix_product,
    filter_through_schema,
    remove_invalid_products,
    deduplicate_related_products,
)
from oaebu_workflows.config import test_fixtures_folder, schema_folder
from observatory.platform.observatory_environment import (
    ObservatoryTestCase,
    compare_lists_of_dicts,
)
from observatory.platform.files import load_jsonl

FIXTURES_FOLDER = os.path.join(test_fixtures_folder(), "onix_utils")


class TestOnixTransformer(ObservatoryTestCase):
    """Tests for the ONIX transformer end to end"""

    filtered_name = "filtered.xml"
    errors_removed_name = "errors_removed.xml"
    normalised_name = "normalised.xml"
    deduplicated_name = "deduplicated.xml"
    elevated_name = "elevated.xml"
    parsed_name = "full.jsonl"
    apply_names_name = "name_applied.jsonl"
    collapsed_name = "collapsed.jsonl"

    test_input_metadata = os.path.join(FIXTURES_FOLDER, "input_metadata.xml")
    test_output_parse_only = os.path.join(FIXTURES_FOLDER, "output_parse_only.jsonl")
    test_output_metadata = os.path.join(FIXTURES_FOLDER, "output_metadata.jsonl")

    def test_e2e(self):
        with TemporaryDirectory() as tempdir:
            # Your code here
            transformer = OnixTransformer(
                input_path=self.test_input_metadata,
                output_dir=tempdir,
                filter_products=True,
                error_removal=True,
                normalise_related_products=True,
                deduplicate_related_products=True,
                elevate_related_products=True,
                add_name_fields=True,
                collapse_subjects=True,
                filter_schema=os.path.join(
                    schema_folder(workflow_module="oapen_metadata_telescope"), "oapen_metadata_filter.json"
                ),
                invalid_products_name="invalid_products.xml",
                save_format="jsonl",
                keep_intermediate=True,
            )
            transformer_output_path = transformer.transform()
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.filtered_name),
                "7e2b13ae1f25c2d09f11e7864c2f0f92",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.errors_removed_name),
                "6f6cd81c6abe047fffc95fd7d105d78e",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.normalised_name),
                "e73e1d4e0eac10d512b343835373be08",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.deduplicated_name),
                "294a88af4897de5f8b66524afe317520",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.elevated_name),
                "087b7ebcb806b560acf1d6fcd303817c",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.parsed_name),
                "033ef3c67663dc042b16be05db2b1a7b",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.apply_names_name),
                "9dc363f29e8f9bdf8c652dd22aed49e0",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, self.collapsed_name),
                "74e4870f5c37a2e9690156b47c3232f7",
                algorithm="md5",
            )
            self.assert_file_integrity(
                os.path.join(transformer.output_dir, "invalid_products.xml"),
                "1ce5155e79ff4e405564038d4520ae3c",
                algorithm="md5",
            )
            compare_lists_of_dicts(
                load_jsonl(self.test_output_metadata),
                load_jsonl(transformer_output_path),
                primary_key="ISBN13",
            )

    def test_save_formats(self):
        """Tests that each of the posible save formats works"""

        def _json_loader(file_path: str):
            with open(file_path, "r") as f:
                return json.load(f)

        for fmt, loader in [("json", _json_loader), ("jsonl", load_jsonl), ("jsonl.gz", load_jsonl)]:
            with TemporaryDirectory() as tempdir:
                transformer = OnixTransformer(
                    input_path=self.test_input_metadata,
                    output_dir=tempdir,
                    save_format=fmt,
                    keep_intermediate=True,
                )
                transformer_output_path = transformer.transform()
                self.assertTrue(os.path.exists(transformer_output_path))
                self.assertTrue(transformer_output_path.endswith(fmt))
                # Try loading the output
                loader(transformer_output_path)


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
            onix_fixtures = test_fixtures_folder(workflow_module="onix_telescope")
            shutil.copy(os.path.join(onix_fixtures, "20210330_CURTINPRESS_ONIX.xml"), input_dir)
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
        test_subjects_input = os.path.join(FIXTURES_FOLDER, "test_subjects_input.json")
        test_subjects_expected = os.path.join(FIXTURES_FOLDER, "test_subjects_expected.json")
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
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": None, "KeyNames": "Doe", "NamesBeforeKey": "John"}
                ]
            },
            {
                "Contributors": [
                    {"PersonName": None, "PersonNameInverted": None, "KeyNames": "Smith", "NamesBeforeKey": "Jane"}
                ]
            },
        ]
        expected_output = [
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
                        "PersonName": "Jane Smith",
                        "PersonNameInverted": "Smith, Jane",
                        "KeyNames": "Smith",
                        "NamesBeforeKey": "Jane",
                    }
                ]
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
        self.assertCountEqual(result[0]["ProductIdentifier"], expected_result[0]["ProductIdentifier"])
        self.assertCountEqual(result[1]["ProductIdentifier"], expected_result[1]["ProductIdentifier"])

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


class TestFilterThroughSchema(unittest.TestCase):
    def test_filter_through_schema(self):
        """Tests the generic use case of the function"""
        input_dict = {
            # Handle a list of dicts
            "things": [
                {"subthing": {"subsubthing": {"subsubsubthing": "1", "unimportant_subsubsubthing": "1"}}},
                {"subthing": {"subsubthing": {"subsubsubthing": "2"}}, "anotherthing": "2"},
            ],
            "unimportant_thing": "unimportant",  # Neglected field
            "important_thing": "important",  # Field to include
        }
        schema = {
            "things": [{"subthing": {"subsubthing": {"subsubsubthing": None}}, "anotherthing": None}],
            "important_thing": None,
        }
        expected_output = {
            "things": [
                {"subthing": {"subsubthing": {"subsubsubthing": "1"}}},
                {"subthing": {"subsubthing": {"subsubsubthing": "2"}}, "anotherthing": "2"},
            ],
            "important_thing": "important",
        }
        self.assertEqual(filter_through_schema(input_dict, schema), expected_output)

    def test_matching_keys(self):
        """Tests that the function correctly processes the input dictionary when all nested keys match the schema"""
        input_dict = {"thing": {"subthing": {"subsubthing": {"subsubsubthing": "1"}}}}
        schema_dict = {"thing": {"subthing": {"subsubthing": {"subsubsubthing": None}}}}
        expected_output = {"thing": {"subthing": {"subsubthing": {"subsubsubthing": "1"}}}}
        self.assertEqual(filter_through_schema(input_dict, schema_dict), expected_output)

    def test_empty_input(self):
        """Tests that the function correctly handles an empty input dictionary"""
        input_dict = {}
        schema_dict = {"thing": {"subthing": {"subsubthing": None}}}
        expected_output = {}
        self.assertEqual(filter_through_schema(input_dict, schema_dict), expected_output)

    def test_no_matching_keys(self):
        """Tests that the function correctly handles an input dictionary with no matching keys in the schema"""
        input_dict = {"thing": {"subthing": {"subsubthing": "1"}}}
        schema_dict = {"other_thing": {"other_subthing": {"other_subsubthing": None}}}
        expected_output = {}
        self.assertEqual(filter_through_schema(input_dict, schema_dict), expected_output)

    def test_edge_case_empty_schema(self):
        """Tests that the function correctly handles an empty schema"""
        input_dict = {"thing": {"subthing": {"subsubthing": "1"}}}
        schema_dict = {}
        expected_output = {}
        self.assertEqual(filter_through_schema(input_dict, schema_dict), expected_output)


class TestRemoveInvalidProducts(unittest.TestCase):
    oapen_metadata_fixtures = test_fixtures_folder(workflow_module="oapen_metadata_telescope")
    valid_parsed_xml = os.path.join(oapen_metadata_fixtures, "parsed_valid.xml")
    invalid_products_removed_xml = os.path.join(oapen_metadata_fixtures, "invalid_products_removed.xml")
    empty_xml = os.path.join(oapen_metadata_fixtures, "empty_download.xml")
    invalid_products_xml = os.path.join(oapen_metadata_fixtures, "invalid_products.xml")

    def test_remove_invalid_products(self):
        """Tests the function used to remove invalid products from an xml file"""
        with NamedTemporaryFile() as invalid_products_file, NamedTemporaryFile() as processed_file:
            remove_invalid_products(
                self.valid_parsed_xml, processed_file.name, invalid_products_file=invalid_products_file.name
            )

            # Make assertions of the processed xml
            with open(processed_file.name) as f:
                processed_xml = f.readlines()
            with open(self.invalid_products_removed_xml) as f:
                assertion_xml = f.readlines()
            self.assertEqual(len(processed_xml), len(assertion_xml))
            self.assertEqual(processed_xml, assertion_xml)

            # Make assertions of the invalid products
            with open(invalid_products_file.name) as f:
                invalid_products_xml = f.readlines()
            with open(self.invalid_products_xml) as f:
                assertion_xml = f.readlines()
            self.assertEqual(len(invalid_products_xml), len(assertion_xml))
            self.assertEqual(invalid_products_xml, assertion_xml)

    def test_empty_xml(self):
        """Tests the function used to remove invalid products from an xml file"""
        with NamedTemporaryFile() as invalid_products_file, NamedTemporaryFile() as processed_file:
            self.assertRaises(
                AttributeError,
                remove_invalid_products,
                self.empty_xml,
                processed_file.name,
                invalid_products_file=invalid_products_file.name,
            )


class TestFindOnixProduct(unittest.TestCase):
    # Tests that the function can extract multiple products from a valid input xml
    valid_input = [
        "<ONIXMessage>",
        "<Product>",
        "<RecordReference>1</RecordReference>",
        "</Product>",
        "<Product>",
        "<RecordReference>2</RecordReference>",
        "</Product>",
        "<Product>",
        "<RecordReference>3</RecordReference>",
        "<SomeOtherField>something else</SomeOtherField>",
        "</Product>",
        "</ONIXMessage>",
    ]

    def test_find_onix_product(self):
        """Test that the function can extract multiple products from a valid input xml"""
        output = find_onix_product(self.valid_input, 1)
        self.assertEqual(output.record_reference, "1")
        self.assertEqual(output.product, {"RecordReference": "1"})
        output = find_onix_product(self.valid_input, 8)
        self.assertEqual(output.record_reference, "3")
        self.assertEqual(output.product, {"RecordReference": "3", "SomeOtherField": "something else"})

    def test_out_of_bounds_supplied(self):
        """Test that errors are thrown when improper input is supplied"""
        self.assertRaisesRegex(IndexError, "not within the length of the file", find_onix_product, self.valid_input, -1)
        self.assertRaisesRegex(
            IndexError, "not within the length of the file", find_onix_product, self.valid_input, len(self.valid_input)
        )

    def test_missing_record_reference(self):
        """Tests that a product without a RecordReference raises a KeyError"""
        all_lines = ["<Product>", "<RandomField>1</RandomField>", "</Product>"]
        self.assertRaises(KeyError, find_onix_product, all_lines, 1)

    def test_empty_product(self):
        """Tests that a product without a RecordReference raises a KeyError"""
        all_lines = ["<Product>", "</Product>"]
        self.assertRaisesRegex(ValueError, "Product field is empty", find_onix_product, all_lines, 1)

    def test_no_product_tags(self):
        """Tests that the function raises a ValueError when <Product> tags are not closed or missing"""
        missing_product_xml = ["<ONIXMessage>", "</ONIXMessage>"]
        with self.assertRaisesRegex(ValueError, "Product not found surrounding line"):
            find_onix_product(missing_product_xml, 0)
        missing_product_xml = ["<ONIXMessage>", "<Product>", "</ONIXMessage>"]
        with self.assertRaisesRegex(ValueError, "Product not found surrounding line"):
            find_onix_product(missing_product_xml, 1)
        missing_product_xml = ["<ONIXMessage>", "</Product>" "</ONIXMessage>"]
        with self.assertRaisesRegex(ValueError, "Product not found surrounding line"):
            find_onix_product(missing_product_xml, 1)


class TestDeduplicateRelatedProducts(unittest.TestCase):
    def test_no_duplicated_related_products(self):
        """Should return the same list if there are no duplicated related products in any of the products"""
        onix_products = [
            {
                "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "1234567890"}],
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "01",
                            "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "0987654321"}],
                        },
                        {
                            "ProductRelationCode": "02",
                            "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "1357924680"}],
                        },
                    ]
                },
            },
            {
                "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "2468135790"}],
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "03",
                            "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "9876543210"}],
                        },
                        {
                            "ProductRelationCode": "04",
                            "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "0246813579"}],
                        },
                    ]
                },
            },
        ]
        result = deduplicate_related_products(onix_products)
        self.assertEqual(result, onix_products)

    def test_remove_duplicated_related_products_fixed(self):
        """Should remove duplicated related products from a single product"""
        onix_products = [
            {
                "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "1234567890"}],
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "01",
                            "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "0987654321"}],
                        },
                        {
                            "ProductRelationCode": "02",
                            "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "0987654321"}],
                        },
                    ]
                },
            }
        ]
        expected_result = [
            {
                "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "1234567890"}],
                "RelatedMaterial": {
                    "RelatedProduct": [
                        {
                            "ProductRelationCode": "02",
                            "ProductIdentifier": [{"ProductIDType": "03", "IDValue": "0987654321"}],
                        }
                    ]
                },
            }
        ]
        result = deduplicate_related_products(onix_products)
        self.assertEqual(result, expected_result)

    def test_empty_input_list(self):
        """Should return an empty list if the input list is empty"""
        onix_products = []
        result = deduplicate_related_products(onix_products)
        self.assertEqual(result, [])
