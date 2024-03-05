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
from unittest.mock import patch

import pendulum
import vcr
from airflow.utils.state import State

from dags.oaebu_workflows.oaebu_partners import partner_from_str
from dags.oaebu_workflows.thoth_telescope.thoth_telescope import (
    DEFAULT_HOST_NAME,
    ThothRelease,
    thoth_download_onix,
    create_dag,
)
from dags.oaebu_workflows.config import test_fixtures_folder, module_file_path
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.url_utils import retry_get_url
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.test_utils import SandboxTestCase, load_and_parse_json
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment


FAKE_PUBLISHER_ID = "fake_publisher_id"


class TestThothTelescope(SandboxTestCase):
    """Tests for the Thoth telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestThothTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        if not self.project_id:
            raise RuntimeError("TEST_GCP_PROJECT_ID must be set")

        if self.data_location == None:
            raise RuntimeError("TEST_GCP_DATA_LOCATION must be set")

        # Fixtures
        fixtures_folder = test_fixtures_folder(workflow_module="thoth_telescope")
        self.download_cassette = os.path.join(fixtures_folder, "thoth_download_cassette.yaml")
        self.test_table = os.path.join(fixtures_folder, "test_table.json")

    def test_dag_structure(self):
        """Test that the ONIX DAG has the correct structure."""

        dag = create_dag(
            dag_id="thoth_telescope_test",
            cloud_workspace=self.fake_cloud_workspace,
            publisher_id=FAKE_PUBLISHER_ID,
            format_specification="onix_3.0::jstor",
        )

        self.assert_dag_structure(
            {
                "check_dependencies": ["make_release"],
                "make_release": ["download", "transform", "bq_load", "add_new_dataset_releases", "cleanup_workflow"],
                "download": ["transform"],
                "transform": ["bq_load"],
                "bq_load": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup_workflow"],
                "cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id="thoth_telescope_test",
                    name="Thoth Telescope",
                    class_name="dags.oaebu_workflows.thoth_telescope.thoth_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publisher_id=FAKE_PUBLISHER_ID, format_specification="onix::oapen"),
                )
            ],
        )
        with env.create():
            dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
            self.assert_dag_load_from_config("thoth_telescope_test", dag_file)

        # Error should be raised for no publisher_id
        env.workflows[0].kwargs = {}
        with env.create():
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load_from_config("onix_workflow_test_dag_load", dag_file)
            msg = cm.exception.args[0]
            self.assertIn("missing 2 required keyword-only arguments", msg)
            self.assertIn("publisher_id", msg)
            self.assertIn("format_specification", msg)

    def test_telescope(self):
        """Test the Thoth telescope end to end."""
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Create the Observatory environment and run tests
        with env.create():
            # Setup Telescope
            execution_date = pendulum.datetime(year=2022, month=12, day=1)
            metadata_partner = partner_from_str("thoth", metadata_partner=True)
            metadata_partner.bq_dataset_id = env.add_dataset()
            dag_id = "thoth_telescope_test"
            api_dataset_id = env.add_dataset()
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=env.cloud_workspace,
                format_specification="onix_3.0::oapen",
                elevate_related_products=True,
                publisher_id=FAKE_PUBLISHER_ID,
                metadata_partner=metadata_partner,
                api_dataset_id=api_dataset_id,
            )

            with env.create_dag_run(dag, execution_date):
                # Check dependencies task
                ti = env.run_task("check_dependencies")
                self.assertEqual(ti.state, State.SUCCESS)

                # Make release task
                ti = env.run_task("make_release")
                self.assertEqual(ti.state, State.SUCCESS)
                release_dict = ti.xcom_pull(task_ids="make_release", include_prior_dates=False)
                expected_release_dict = {
                    "dag_id": "thoth_telescope_test",
                    "run_id": "scheduled__2022-12-01T00:00:00+00:00",
                    "snapshot_date": "2022-12-04",
                }
                self.assertEqual(release_dict, expected_release_dict)
                release = ThothRelease.from_dict(release_dict)

                # Download task
                # Ignore the googleapis host so the upload step works
                thoth_vcr = vcr.VCR(
                    record_mode="none", ignore_hosts=["oauth2.googleapis.com", "storage.googleapis.com"]
                )
                with thoth_vcr.use_cassette(self.download_cassette):
                    ti = env.run_task("download")
                    self.assertEqual(ti.state, State.SUCCESS)

                # Transform task
                ti = env.run_task("transform")
                self.assertEqual(ti.state, State.SUCCESS)

                # Bigquery load task
                ti = env.run_task("bq_load")
                self.assertEqual(ti.state, State.SUCCESS)

                ### Make assertions ###

                # Downloaded file
                self.assert_file_integrity(release.download_path, "043e9c474e14e2776b22fc590ea1773c", "md5")

                # Uploaded download blob
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Uploaded transform blob
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )

                # Uploaded table
                table_id = bq_sharded_table_id(
                    env.cloud_workspace.project_id,
                    metadata_partner.bq_dataset_id,
                    metadata_partner.bq_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, expected_rows=5)
                self.assert_table_content(table_id, load_and_parse_json(self.test_table), primary_key="ISBN13")

                # Set up the API
                api = DatasetAPI(project_id=self.project_id)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)

                now = pendulum.now("Europe/London")  # Use Europe/London to ensure +00UTC timezone
                with patch("dags.oaebu_workflows.thoth_telescope.thoth_telescope.pendulum.now") as mock_now:
                    mock_now.return_value = now
                    ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "dataset_id": api_dataset_id,
                    "dag_run_id": release.run_id,
                    "created": now.to_iso8601_string(),
                    "modified": now.to_iso8601_string(),
                    "data_interval_start": "2022-12-01T00:00:00+00:00",
                    "data_interval_end": "2022-12-04T00:00:00+00:00",
                    "snapshot_date": "2022-12-04T00:00:00+00:00",
                    "partition_date": None,
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": "null",
                }
                self.assertEqual(expected_release, dataset_releases[0].to_dict())

                # Test cleanup
                workflow_folder_path = release.workflow_folder
                ti = env.run_task("cleanup_workflow")
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(workflow_folder_path)

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
                os.path.join(tempdir, "fake_download.xml"), "043e9c474e14e2776b22fc590ea1773c", "md5"
            )

    def test_thoth_api(self):
        """Tests that HTTP requests to the thoth API are successful"""
        base_response = retry_get_url(DEFAULT_HOST_NAME, num_retries=2)
        format_response = retry_get_url(f"{DEFAULT_HOST_NAME}/specifications/onix_3.0::oapen", num_retries=2)
        self.assertEqual(base_response.status_code, 200)
        self.assertEqual(format_response.status_code, 200)
