# Copyright 2020-2023 Curtin University
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
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.oapen_metadata_telescope.oapen_metadata_telescope import (
    OapenMetadataTelescope,
    download_metadata,
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

        fixtures_folder = test_fixtures_folder(workflow_module="oapen_metadata_telescope")
        self.valid_download_cassette = os.path.join(fixtures_folder, "cassette_valid.yaml")  # VCR Cassette
        self.test_table = os.path.join(fixtures_folder, "test_table.json")  # File for testing final table

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
                    class_name="oaebu_workflows.oapen_metadata_telescope.oapen_metadata_telescope.OapenMetadataTelescope",
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
            partner = partner_from_str("oapen_metadata", metadata_partner=True)
            partner.bq_dataset_id = dataset_id
            telescope = OapenMetadataTelescope(
                dag_id="oapen_metadata",
                cloud_workspace=env.cloud_workspace,
                metadata_uri=self.metadata_uri,
                metadata_partner=partner,
                elevate_related_products=True,
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
                self.assert_file_integrity(release.download_path, "c246a8f7487de756f4dd47cd0ab94363", "md5")

                # Test that download file uploaded to BQ
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Test transform task produced the files we care about
                invalid_products_path = os.path.join(release.transform_folder, "invalid_products.xml")
                self.assertTrue(os.path.exists(invalid_products_path))

                # Check file content is as expected
                self.assert_file_integrity(invalid_products_path, "1ce5155e79ff4e405564038d4520ae3c", "md5")

                # Test that transformed files uploaded to BQ
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(invalid_products_path), invalid_products_path
                )

                # Test that table is loaded to BQ
                table_id = bq_sharded_table_id(
                    telescope.cloud_workspace.project_id,
                    telescope.metadata_partner.bq_dataset_id,
                    telescope.metadata_partner.bq_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, expected_rows=5)
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
    fixtures_folder = test_fixtures_folder(workflow_module="oapen_metadata_telescope")
    valid_download_cassette = os.path.join(fixtures_folder, "cassette_valid.yaml")
    invalid_download_cassette = os.path.join(fixtures_folder, "cassette_invalid.yaml")
    empty_download_cassette = os.path.join(fixtures_folder, "cassette_empty.yaml")
    bad_response_cassette = os.path.join(fixtures_folder, "cassette_bad_response.yaml")
    header_only_download_cassette = os.path.join(fixtures_folder, "cassette_header_only.yaml")

    # XMLs
    valid_download_xml = os.path.join(fixtures_folder, "metadata_download_valid.xml")

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
        with open(self.valid_download_xml, "r") as f:  # Note - do not format this file or this test will fail
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
