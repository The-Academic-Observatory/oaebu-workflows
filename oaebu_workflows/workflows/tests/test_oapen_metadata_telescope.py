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
from unittest.mock import MagicMock
from tempfile import NamedTemporaryFile
from xml.etree import ElementTree
import json

import pendulum
import vcr
from airflow.exceptions import AirflowException
from airflow.utils.state import State

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oapen_metadata_telescope import (
    OapenMetadataTelescope,
    download_oapen_metadata,
    oapen_metadata_parse,
    remove_invalid_products,
    find_onix_product,
    process_xml_element,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.observatory_environment import ObservatoryEnvironment, ObservatoryTestCase, find_free_port


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

        # VCR Cassettes
        self.valid_download_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_valid.yaml")
        self.invalid_download_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_invalid.yaml")
        self.bad_response_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_bad_response.yaml")
        self.empty_download_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_empty.yaml")
        self.header_only_download_cassette = test_fixtures_folder(
            "oapen_metadata", "oapen_metadata_cassette_header_only.yaml"
        )

        # XML files for testing
        self.valid_download_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_download_valid.xml")
        self.invalid_download_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_download_invalid.xml")
        self.empty_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_download_empty.xml")
        self.valid_parsed_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_parsed_valid.xml")
        self.invalid_parsed_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_parsed_invalid.xml")
        self.valid_processed_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_processed_valid.xml")
        self.processing_test_before = test_fixtures_folder("oapen_metadata", "processing_test_before.xml")
        self.processing_test_after = test_fixtures_folder("oapen_metadata", "processing_test_after.xml")
        self.invalid_products_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_invalid_products.xml")
        # Valid fields json for the xml processing test
        self.processing_test_fields = test_fixtures_folder("oapen_metadata", "processing_test_valid_fields.json")

    def test_dag_structure(self):
        """Test that the Oapen Metadata DAG has the correct structure"""
        dag = OapenMetadataTelescope(
            dag_id="oapen_metadata",
            cloud_workspace=self.fake_cloud_workspace,
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
                self.assertEqual(1, len(os.listdir(os.path.dirname(release.download_path))))
                self.assert_file_integrity(release.download_path, "6df963cd448fe3ec8acb76bf49b34928", "md5")

                # Test that download file uploaded to BQ
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Test transform task
                # release.transform_path -> oapen_onix_20210206.jsonl.gz
                # release.invalid_products_path -> oapen_onix_invalid_products_20210206.xml
                # release.post_parse_onix -> full.jsonl
                self.assertTrue(os.path.exists(release.transform_path))
                self.assertTrue(os.path.exists(release.invalid_products_path))
                self.assertTrue(os.path.exists(release.post_parse_onix))
                self.assert_file_integrity(release.transform_path, "e3474ca2", "gzip_crc")
                self.assert_file_integrity(release.invalid_products_path, "297173aa0a09aa1dc538eadfc48285c5", "md5")
                self.assert_file_integrity(release.post_parse_onix, "ecf04d998a7110d80d2eab47d058cfac", "md5")

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
                    env.transform_bucket, gcs_blob_name_from_path(release.post_parse_onix), release.post_parse_onix
                )

                # Test that table is loaded to BQ
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_sharded_table_id(
                    telescope.cloud_workspace.project_id,
                    telescope.bq_dataset_id,
                    telescope.bq_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, expected_rows=2)

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

    def test_download_oapen_metadata(self):
        """Tests the function used to download the oapen metadata xml file"""
        download_file = NamedTemporaryFile(delete=False)

        # Remove the wait time before retries for testing
        download_oapen_metadata.retry.sleep = MagicMock()

        # For a valid XML
        with vcr.VCR().use_cassette(self.valid_download_cassette, record_mode="none", allow_playback_repeats=True):
            download_oapen_metadata(download_file.name)
        with open(download_file.name, "r") as f:
            downloaded_xml = f.readlines()
        with open(self.valid_download_xml, "r") as f:
            assertion_xml = f.readlines()
        assert len(downloaded_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(downloaded_xml)} lines. Expected {len(assertion_xml)}"
        assert downloaded_xml == assertion_xml, "Downloaded 'valid' XML is not equal to the expected XML"

        # For invalid XML
        with vcr.VCR().use_cassette(self.invalid_download_cassette, record_mode="none", allow_playback_repeats=True):
            self.assertRaises(ElementTree.ParseError, download_oapen_metadata, download_file.name)

        # For empty XML
        with vcr.VCR().use_cassette(self.empty_download_cassette, record_mode="none", allow_playback_repeats=True):
            self.assertRaises(ElementTree.ParseError, download_oapen_metadata, download_file.name)

        # For only-header XML
        with vcr.VCR().use_cassette(
            self.header_only_download_cassette, record_mode="none", allow_playback_repeats=True
        ):
            self.assertRaises(AirflowException, download_oapen_metadata, download_file.name)

        # For non-200 response code
        with vcr.VCR().use_cassette(self.bad_response_cassette, record_mode="none", allow_playback_repeats=True):
            self.assertRaises(ConnectionError, download_oapen_metadata, download_file.name)

    def test_oapen_metadata_parse(self):
        """Tests the function used to parse the relevant fields into an onix file"""
        telescope = OapenMetadataTelescope(
            dag_id="oapen_metadata",
            cloud_workspace=self.fake_cloud_workspace,
        )
        with open(telescope.onix_product_fields_file) as f:
            onix_product_fields = json.load(f)
        with open(telescope.onix_header_fields_file) as f:
            onix_header_fields = json.load(f)
        parsed_file = NamedTemporaryFile(delete=False)

        # For a valid XML
        oapen_metadata_parse(
            self.valid_download_xml,
            parsed_file.name,
            onix_product_fields=onix_product_fields,
            onix_header_fields=onix_header_fields,
        )
        with open(parsed_file.name, "r") as f:
            parsed_xml = f.readlines()
        with open(self.valid_parsed_xml, "r") as f:
            assertion_xml = f.readlines()
        assert len(parsed_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(parsed_xml)} lines. Expected {len(assertion_xml)}"
        assert parsed_xml == assertion_xml, "Parsed 'valid' XML is not equal to the expected XML"

        # For invalid XML
        self.assertRaises(
            ElementTree.ParseError,
            oapen_metadata_parse,
            self.invalid_download_xml,
            parsed_file.name,
            onix_product_fields=onix_product_fields,
            onix_header_fields=onix_header_fields,
        )

        # For empty XML
        self.assertRaises(
            ElementTree.ParseError,
            oapen_metadata_parse,
            self.empty_xml,
            parsed_file.name,
            onix_product_fields=onix_product_fields,
            onix_header_fields=onix_header_fields,
        )

    def test_remove_invalid_products(self):
        """Tests the function used to remove invalid products from an xml file"""
        processed_file = NamedTemporaryFile(delete=False)
        invalid_products_file = NamedTemporaryFile(delete=False)

        # For a valid XML
        remove_invalid_products(
            self.valid_parsed_xml, processed_file.name, invalid_products_file=invalid_products_file.name
        )
        with open(processed_file.name) as f:
            processed_xml = f.readlines()
        with open(self.valid_processed_xml) as f:
            assertion_xml = f.readlines()
        assert len(processed_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(processed_xml)} lines. Expected {len(assertion_xml)}"
        assert processed_xml == assertion_xml, "Processed 'valid' XML is not equal to the expected XML"
        # Check the invalid products xml
        with open(invalid_products_file.name) as f:
            invalid_products_xml = f.readlines()
        with open(self.invalid_products_xml) as f:
            assertion_xml = f.readlines()
        assert len(invalid_products_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(invalid_products_xml)} lines. Expected {len(assertion_xml)}"
        assert (
            invalid_products_xml == assertion_xml
        ), "Generated 'invalid products' XML is not equal to the expected XML"

        # For an invalid XML
        self.assertRaises(AttributeError, remove_invalid_products, self.invalid_parsed_xml, processed_file.name)

        # For an empty XML
        self.assertRaises(AttributeError, remove_invalid_products, self.invalid_parsed_xml, processed_file.name)

    def test_find_onix_product(self):
        """Tests the function that determines the line range of a product that encompasses a given line"""
        with open(self.valid_parsed_xml) as f:
            all_lines = f.readlines()

        # Make assertions [(line_number, (expected_output_range))]
        assertions = [(150, (9, 165)), (170, (166, 224)), (225, (166, 224)), (226, (226, 289))]
        for line, product_range in assertions:
            output_range = find_onix_product(all_lines, line)
            assert (
                output_range == product_range
            ), f"Product range not as expected for inpiut line {line}. Expected {product_range}, got {output_range}"

        self.assertRaises(AirflowException, find_onix_product, all_lines, 1)
        self.assertRaises(IndexError, find_onix_product, all_lines, int(1e10))

    def test_process_xml_element(self):
        """Tests the function that removes undesired fields from an xml recusrively"""
        with open(self.processing_test_fields) as f:
            viable_fields = json.load(f)

        # Process the input xml
        tree = ElementTree.parse(self.processing_test_before)
        test_xml = ElementTree.Element(tree.getroot().tag)
        process_xml_element(tree.getroot(), viable_fields, test_xml)

        # Canonicalize the expected and test xmls and compare them
        expected_xml = ElementTree.canonicalize(from_file=self.processing_test_after, strip_text=True)
        test_xml = ElementTree.canonicalize(ElementTree.tostring(test_xml), strip_text=True)
        assert (
            expected_xml == test_xml
        ), f"Processed XML is not equal to expected XML. Expected {expected_xml}, got {test_xml}"
