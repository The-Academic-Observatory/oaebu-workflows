# Copyright 2022-2024 Curtin University
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
from unittest.mock import patch

import pendulum
import vcr
from airflow.utils.state import State
from airflow.models.connection import Connection

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.irus_fulcrum_telescope.irus_fulcrum_telescope import (
    IrusFulcrumRelease,
    create_dag,
    download_fulcrum_month_data,
    transform_fulcrum_data,
)
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.config import module_file_path
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.files import load_jsonl
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.google.bigquery import bq_table_id
from observatory_platform.sandbox.test_utils import SandboxTestCase, load_and_parse_json
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment

FAKE_PUBLISHERS = ["Fake Publisher 1", "Fake Publisher 2", "Fake Publisher 3"]


class TestIrusFulcrumTelescope(SandboxTestCase):
    """Tests for the Fulcrum telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # Fixtures
        fixtures_folder = test_fixtures_folder(workflow_module="irus_fulcrum_telescope")
        self.download_cassette = os.path.join(fixtures_folder, "fulcrum_download_cassette.yaml")
        self.test_table = os.path.join(fixtures_folder, "test_final_table.json")
        self.test_totals_download = os.path.join(fixtures_folder, "test_totals_download.jsonl")
        self.test_country_download = os.path.join(fixtures_folder, "test_country_download.jsonl")
        self.test_transform = os.path.join(fixtures_folder, "test_transform.jsonl")

    def test_dag_structure(self):
        """Test that the ONIX DAG has the correct structure and raises errors when necessary"""
        dag = create_dag(dag_id="fulcrum_test", cloud_workspace=self.fake_cloud_workspace, publishers=FAKE_PUBLISHERS)
        self.assert_dag_structure(
            {
                "check_dependencies": ["make_release"],
                "make_release": ["transform", "cleanup_workflow", "download", "add_new_dataset_releases", "bq_load"],
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
                    dag_id="fulcrum_test",
                    name="Fulcrum Telescope",
                    class_name="oaebu_workflows.irus_fulcrum_telescope.irus_fulcrum_telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publishers=[FAKE_PUBLISHERS]),
                )
            ]
        )
        with env.create():
            dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
            self.assert_dag_load_from_config("fulcrum_test", dag_file)

    def test_telescope(self):
        """Test the Fulcrum telescope end to end."""

        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Create the Observatory environment and run tests
        with env.create():
            # Setup DAG
            logical_date = pendulum.datetime(year=2022, month=4, day=7)
            data_partner = partner_from_str("irus_fulcrum")
            data_partner.bq_dataset_id = env.add_dataset()
            api_bq_dataset_id = env.add_dataset()
            dag_id = "fulcrum_test"
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=env.cloud_workspace,
                publishers=FAKE_PUBLISHERS,
                data_partner=data_partner,
                api_bq_dataset_id=api_bq_dataset_id,
            )
            env.add_connection(Connection(conn_id="irus_api", uri=f"http://fake_api_login:@"))

            # Add the fake requestor ID as a connection
            with env.create_dag_run(dag, logical_date=logical_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(ti.state, State.SUCCESS)

                # Test that make release is successful
                ti = env.run_task("make_release")
                self.assertEqual(ti.state, State.SUCCESS)
                release_dict = ti.xcom_pull(task_ids="make_release", include_prior_dates=False)
                expected_release_dict = {
                    "dag_id": "fulcrum_test",
                    "run_id": "scheduled__2022-04-07T00:00:00+00:00",
                    "data_interval_start": "2022-04-01",
                    "data_interval_end": "2022-05-01",
                    "partition_date": "2022-04-30",
                }
                self.assertEqual(release_dict, expected_release_dict)

                # Test download
                # Ignore the googleapis host so the upload step works
                fulcrum_vcr = vcr.VCR(
                    record_mode="none", ignore_hosts=["oauth2.googleapis.com", "storage.googleapis.com"]
                )
                with fulcrum_vcr.use_cassette(self.download_cassette):
                    ti = env.run_task("download")
                    self.assertEqual(ti.state, State.SUCCESS)

                # Test transform
                ti = env.run_task("transform")
                self.assertEqual(ti.state, State.SUCCESS)

                # Test load into BigQuery
                ti = env.run_task("bq_load")
                self.assertEqual(ti.state, State.SUCCESS)

                ### Make assertions ##

                # Create the release
                release = IrusFulcrumRelease.from_dict(release_dict)

                # Downloaded files
                self.assert_file_integrity(release.download_totals_path, "95b7dceb", "gzip_crc")
                self.assert_file_integrity(release.download_country_path, "0a713d03", "gzip_crc")

                # Uploaded download blob
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_totals_path),
                    release.download_totals_path,
                )
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_country_path),
                    release.download_country_path,
                )

                # Transformed file
                self.assert_file_integrity(release.transform_path, "40a25e4e", "gzip_crc")

                # Uploaded transform blob
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )

                # Uploaded table
                table_id = bq_table_id(
                    env.cloud_workspace.project_id, data_partner.bq_dataset_id, data_partner.bq_table_name
                )
                self.assert_table_integrity(table_id, expected_rows=3)
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(self.test_table, date_fields="release_date"),
                    primary_key="proprietary_id",
                )

                # Set up the API
                api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=api_bq_dataset_id)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="irus_fulcrum")
                self.assertEqual(len(dataset_releases), 0)

                # Add_dataset_release_task
                now = pendulum.now("UTC")
                with patch("oaebu_workflows.irus_fulcrum_telescope.irus_fulcrum_telescope.pendulum.now") as mock_now:
                    mock_now.return_value = now
                    ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="irus_fulcrum")
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "entity_id": "irus_fulcrum",
                    "dag_run_id": release.run_id,
                    "created": now.to_iso8601_string(),
                    "modified": now.to_iso8601_string(),
                    "data_interval_start": "2022-04-01T00:00:00Z",
                    "data_interval_end": "2022-05-01T00:00:00Z",
                    "snapshot_date": None,
                    "partition_date": "2022-04-30T00:00:00Z",
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": {},
                }
                self.assertEqual(expected_release, dataset_releases[0].to_dict())

                # Test cleanup
                workflow_folder_path = release.workflow_folder
                ti = env.run_task("cleanup_workflow")
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(workflow_folder_path)

    def test_download_fulcrum_month_data(self):
        """Tests the download_fuclrum_month_data function"""
        vcr_ = vcr.VCR(record_mode="none")
        with vcr_.use_cassette(self.download_cassette):
            actual_totals, actual_country = download_fulcrum_month_data(
                download_month=pendulum.datetime(year=2022, month=4, day=15),
                requestor_id="fake_api_login",
                num_retries=0,
            )
        expected_totals = load_jsonl(self.test_totals_download)
        expected_country = load_jsonl(self.test_country_download)

        # Make list order deterministic before testing
        actual_totals = [dict(sorted(d.items())) for d in actual_totals]
        actual_country = [dict(sorted(d.items())) for d in actual_country]
        expected_totals = [dict(sorted(d.items())) for d in expected_totals]
        expected_country = [dict(sorted(d.items())) for d in expected_country]
        self.assertListEqual(actual_totals, expected_totals)
        self.assertListEqual(actual_country, expected_country)

    def test_transform_fulcrum_data(self):
        """Tests the transform_fulcrum_data function"""
        totals = load_jsonl(self.test_totals_download)
        country = load_jsonl(self.test_country_download)
        actual_transform = transform_fulcrum_data(totals_data=totals, country_data=country, publishers=FAKE_PUBLISHERS)
        expected_transform = load_jsonl(self.test_transform)

        # Make list order deterministic before testing
        actual_transform = [dict(sorted(d.items())) for d in actual_transform]
        expected_transform = [dict(sorted(d.items())) for d in expected_transform]
        self.assertListEqual(actual_transform, expected_transform)
