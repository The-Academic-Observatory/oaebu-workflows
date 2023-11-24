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
from tempfile import TemporaryDirectory

import pendulum
import vcr
from airflow.utils.state import State

from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.thoth_telescope.thoth_telescope import (
    ThothTelescope,
    thoth_download_onix,
    DEFAULT_HOST_NAME,
)
from oaebu_workflows.config import test_fixtures_folder
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.utils.url_utils import retry_get_url
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    load_and_parse_json,
)


FAKE_PUBLISHER_ID = "fake_publisher_id"


class TestThothTelescope(ObservatoryTestCase):
    """Tests for the Thoth telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestThothTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # Fixtures
        fixtures_folder = test_fixtures_folder(workflow_module="thoth_telescope")
        self.download_cassette = os.path.join(fixtures_folder, "thoth_download_cassette.yaml")
        self.test_table = os.path.join(fixtures_folder, "test_table.json")

    def test_dag_structure(self):
        """Test that the ONIX DAG has the correct structure."""

        dag = ThothTelescope(
            dag_id="thoth_telescope_test",
            cloud_workspace=self.fake_cloud_workspace,
            publisher_id=FAKE_PUBLISHER_ID,
            format_specification="onix_3.0::jstor",
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
        """Test that the DAG can be loaded from a DAG bag."""
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="thoth_telescope_test",
                    name="Thoth Telescope",
                    class_name="oaebu_workflows.thoth_telescope.thoth_telescope.ThothTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publisher_id=FAKE_PUBLISHER_ID, format_specification="onix::oapen"),
                )
            ],
        )
        with env.create():
            self.assert_dag_load_from_config("thoth_telescope_test")

        # Error should be raised for no publisher_id
        env.workflows[0].kwargs = {}
        with env.create():
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load_from_config("onix_workflow_test_dag_load")
            msg = cm.exception.args[0]
            self.assertIn("missing 2 required keyword-only arguments", msg)
            self.assertIn("publisher_id", msg)
            self.assertIn("format_specification", msg)

    def test_telescope(self):
        """Test the Thoth telescope end to end."""
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )

        # Create the Observatory environment and run tests
        with env.create():
            # Setup Telescope
            execution_date = pendulum.datetime(year=2022, month=12, day=1)
            metadata_partner = partner_from_str("thoth", metadata_partner=True)
            metadata_partner.bq_dataset_id = env.add_dataset()
            telescope = ThothTelescope(
                dag_id="thoth_telescope_test",
                cloud_workspace=env.cloud_workspace,
                format_specification="onix_3.0::oapen",
                publisher_id=FAKE_PUBLISHER_ID,
                metadata_partner=metadata_partner,
            )
            dag = telescope.make_dag()

            with env.create_dag_run(dag, execution_date):
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                thoth_vcr = vcr.VCR(record_mode="none")
                with thoth_vcr.use_cassette(self.download_cassette):
                    ti = env.run_task(telescope.download.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                ### Make assertions ###

                # Make the release
                release = telescope.make_release(
                    run_id=env.dag_run.run_id, data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end))
                )

                # Downloaded file
                self.assert_file_integrity(release.download_path, "6d0f31c315dab144054e2fde9ad7f8ab", "md5")

                # Uploaded download blob
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Transformed file
                self.assert_file_integrity(release.transform_path, "1b12c3a9", "gzip_crc")

                # Uploaded transform blob
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )

                # Uploaded table
                table_id = bq_sharded_table_id(
                    telescope.cloud_workspace.project_id,
                    telescope.metadata_partner.bq_dataset_id,
                    telescope.metadata_partner.bq_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, expected_rows=2)
                self.assert_table_content(table_id, load_and_parse_json(self.test_table), primary_key="ISBN13")

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test cleanup
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(release.workflow_folder)

    # Function tests
    def test_download_onix(self):
        """
        Tests the download_onix function.
        Will test that the function works as expected using proxy HTTP requests
        """
        with TemporaryDirectory() as tempdir:
            thoth_vcr = vcr.VCR(record_mode="none")
            with thoth_vcr.use_cassette(self.download_cassette):
                thoth_download_onix(
                    FAKE_PUBLISHER_ID,
                    download_path=os.path.join(tempdir, "fake_download.xml"),
                    num_retries=0,
                    format_spec="onix_3.0::oapen",
                )
            self.assert_file_integrity(
                os.path.join(tempdir, "fake_download.xml"), "6d0f31c315dab144054e2fde9ad7f8ab", "md5"
            )

    def test_thoth_api(self):
        """Tests that HTTP requests to the thoth API are successful"""
        base_response = retry_get_url(DEFAULT_HOST_NAME, num_retries=2)
        format_response = retry_get_url(f"{DEFAULT_HOST_NAME}/specifications/onix_3.0::oapen", num_retries=2)
        self.assertEqual(base_response.status_code, 200)
        self.assertEqual(format_response.status_code, 200)
