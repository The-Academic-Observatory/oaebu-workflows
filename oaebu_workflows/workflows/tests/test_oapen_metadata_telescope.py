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

# Author: Aniek Roelofs, Tuan Chien, Keegan Smith

import os
import unittest
from unittest.mock import MagicMock
from tempfile import NamedTemporaryFile
from xml.parsers.expat import ExpatError

import pendulum
import vcr
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from tenacity import stop_after_attempt

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oapen_metadata_telescope import (
    OapenMetadataTelescope,
    download_metadata,
    remove_invalid_products,
    find_onix_product,
    filter_through_schema,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    load_and_parse_json,
)


class TestOapenMetadataTelescope(ObservatoryTestCase):
    """Tests for the Oapen Metadata Telescope DAG"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.metadata_uri = "https://library.oapen.org/download-export?format=onix"  # metadata URI
        self.valid_download_cassette = test_fixtures_folder("oapen_metadata", "cassette_valid.yaml")  # VCR Cassette
        self.test_table = test_fixtures_folder("oapen_metadata", "test_table.json")  # File for testing final table

    def test_dag_structure(self):
        """Test that the Oapen Metadata DAG has the correct structure"""
        dag = OapenMetadataTelescope(
            dag_id="oapen_metadata",
            cloud_workspace=self.fake_cloud_workspace,
            metadata_uri="",
        ).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
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
        """Test that the OapenMetadata DAG can be loaded from a DAG bag"""
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="oapen_metadata",
                    name="OAPEN Metadata Telescope",
                    class_name="oaebu_workflows.workflows.oapen_metadata_telescope.OapenMetadataTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(metadata_uri=""),
                )
            ],
        )
        with env.create():
            self.assert_dag_load_from_config("oapen_metadata")

    def test_telescope(self):
        """Test telescope task execution."""

        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        dataset_id = env.add_dataset()

        with env.create():
            telescope = OapenMetadataTelescope(
                dag_id="oapen_metadata",
                cloud_workspace=env.cloud_workspace,
                metadata_uri=self.metadata_uri,
                bq_dataset_id=dataset_id,
            )
            dag = telescope.make_dag()

            # first run
            with env.create_dag_run(dag, pendulum.datetime(year=2021, month=2, day=1)):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Download task
                with vcr.VCR().use_cassette(self.valid_download_cassette, record_mode="None"):
                    ti = env.run_task(telescope.download.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                # Upload download task
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Transform task
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Upload transform task
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Bigquery load task
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                ### Make Assertions ###

                # Create the release
                release = telescope.make_release(
                    run_id=env.dag_run.run_id, data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end))
                )

                # Test download task
                self.assertTrue(os.path.exists(release.download_path))
                self.assert_file_integrity(release.download_path, "6df963cd448fe3ec8acb76bf49b34928", "md5")

                # Test that download file uploaded to BQ
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Test transform task
                self.assertTrue(os.path.exists(release.filtered_metadata))
                self.assertTrue(os.path.exists(release.validated_onix))
                self.assertTrue(os.path.exists(release.invalid_products_path))
                self.assertTrue(os.path.exists(release.parsed_onix))
                self.assertTrue(os.path.exists(release.transform_path))

                # Test that transformed files uploaded to BQ
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.invalid_products_path),
                    release.invalid_products_path,
                )
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.parsed_onix), release.parsed_onix
                )

                # Test that table is loaded to BQ
                table_id = bq_sharded_table_id(
                    telescope.cloud_workspace.project_id,
                    telescope.bq_dataset_id,
                    telescope.bq_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, expected_rows=2)
                self.assert_table_content(table_id, load_and_parse_json(self.test_table), primary_key="ISBN13")

                # Add_dataset_release_task
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test that all data deleted
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(release.workflow_folder)


class TestDownloadMetadata(unittest.TestCase):
    # Cassettes
    valid_download_cassette = test_fixtures_folder("oapen_metadata", "cassette_valid.yaml")
    invalid_download_cassette = test_fixtures_folder("oapen_metadata", "cassette_invalid.yaml")
    empty_download_cassette = test_fixtures_folder("oapen_metadata", "cassette_empty.yaml")
    bad_response_cassette = test_fixtures_folder("oapen_metadata", "cassette_bad_response.yaml")
    header_only_download_cassette = test_fixtures_folder("oapen_metadata", "cassette_header_only.yaml")

    # XMLs
    valid_download_xml = test_fixtures_folder("oapen_metadata", "metadata_download_valid.xml")

    # Download URI
    uri = "https://library.oapen.org/download-export?format=onix"

    # Remove the wait time before retries for testing
    download_metadata.retry.sleep = MagicMock()

    def test_download_metadata(self):
        """Test that metadata successfully downloads after 200 respose"""
        with vcr.VCR().use_cassette(self.valid_download_cassette, record_mode="none", allow_playback_repeats=True):
            with NamedTemporaryFile() as download_file:
                download_metadata(self.uri, download_file.name)
                with open(download_file.name, "r") as f:
                    downloaded_xml = f.readlines()
        with open(self.valid_download_xml, "r") as f:
            assertion_xml = f.readlines()
        self.assertEqual(len(downloaded_xml), len(assertion_xml))
        self.assertEqual(downloaded_xml, assertion_xml)

    def test_download_metadata_invalid_xml(self):
        """Test behaviour when the downloaded file is an invalid XML"""
        download_metadata.retry.stop = stop_after_attempt(1)
        with vcr.VCR().use_cassette(self.invalid_download_cassette, record_mode="none", allow_playback_repeats=True):
            with NamedTemporaryFile() as download_file:
                self.assertRaises(ExpatError, download_metadata, self.uri, download_file.name)

    def test_download_metadata_empty_xml(self):
        """Test behaviour when the downloaded file is an empty XML"""
        download_metadata.retry.stop = stop_after_attempt(1)
        with vcr.VCR().use_cassette(self.empty_download_cassette, record_mode="none", allow_playback_repeats=True):
            with NamedTemporaryFile() as download_file:
                self.assertRaises(ExpatError, download_metadata, self.uri, download_file.name)

    def test_download_metadata_no_products(self):
        """Test behaviour when the downloaded file is an empty XML"""
        download_metadata.retry.stop = stop_after_attempt(1)
        # For only-header XML
        with vcr.VCR().use_cassette(
            self.header_only_download_cassette, record_mode="none", allow_playback_repeats=True
        ):
            with NamedTemporaryFile() as download_file:
                self.assertRaisesRegex(
                    AirflowException, "No products found", download_metadata, self.uri, download_file.name
                )

    def test_download_metadata_bad_response(self):
        """Test behaviour when the downloaded file has a non-200 response code"""
        download_metadata.retry.stop = stop_after_attempt(1)
        with vcr.VCR().use_cassette(self.bad_response_cassette, record_mode="none", allow_playback_repeats=True):
            with NamedTemporaryFile() as download_file:
                self.assertRaisesRegex(
                    ConnectionError, "Expected status code 200", download_metadata, self.uri, download_file.name
                )


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
    valid_parsed_xml = test_fixtures_folder("oapen_metadata", "parsed_valid.xml")
    invalid_products_removed_xml = test_fixtures_folder("oapen_metadata", "invalid_products_removed.xml")
    empty_xml = test_fixtures_folder("oapen_metadata", "empty_download.xml")
    invalid_products_xml = test_fixtures_folder("oapen_metadata", "invalid_products.xml")

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
