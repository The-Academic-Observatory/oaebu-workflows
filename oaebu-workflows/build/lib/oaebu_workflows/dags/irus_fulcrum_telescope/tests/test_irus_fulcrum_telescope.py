# Copyright 2022-2023 Curtin University
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

import pendulum
import vcr
from airflow.utils.state import State
from airflow.models.connection import Connection

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.irus_fulcrum_telescope.irus_fulcrum_telescope import (
    IrusFulcrumTelescope,
    download_fulcrum_month_data,
    transform_fulcrum_data,
)
from observatory.platform.files import load_jsonl
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    load_and_parse_json,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.bigquery import bq_table_id
from observatory.platform.observatory_config import Workflow

FAKE_PUBLISHERS = ["Fake Publisher 1", "Fake Publisher 2", "Fake Publisher 3"]


class TestIrusFulcrumTelescope(ObservatoryTestCase):
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
        dag = IrusFulcrumTelescope(
            dag_id="fulcrum_test", cloud_workspace=self.fake_cloud_workspace, publishers=FAKE_PUBLISHERS
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
                    dag_id="fulcrum_test",
                    name="Fulcrum Telescope",
                    class_name="oaebu_workflows.irus_fulcrum_telescope.irus_fulcrum_telescope.IrusFulcrumTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publishers=[FAKE_PUBLISHERS]),
                )
            ]
        )
        with env.create():
            self.assert_dag_load_from_config("fulcrum_test")

    def test_telescope(self):
        """Test the Fulcrum telescope end to end."""
        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        # Create the Observatory environment and run tests
        with env.create():
            # Setup Telescope
            execution_date = pendulum.datetime(year=2022, month=4, day=7)
            partner = partner_from_str("irus_fulcrum")
            partner.bq_dataset_id = env.add_dataset()
            telescope = IrusFulcrumTelescope(
                dag_id="fulcrum_test",
                cloud_workspace=env.cloud_workspace,
                publishers=FAKE_PUBLISHERS,
                data_partner=partner,
            )
            dag = telescope.make_dag()
            env.add_connection(Connection(conn_id=telescope.irus_oapen_api_conn_id, uri=f"http://fake_api_login:@"))

            # Add the fake requestor ID as a connection
            with env.create_dag_run(dag, execution_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test download
                fulcrum_vcr = vcr.VCR(record_mode="none")
                with fulcrum_vcr.use_cassette(self.download_cassette):
                    ti = env.run_task(telescope.download.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                # Test upload downloaded
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test transform
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test upload to cloud storage
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test load into BigQuery
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                ### Make assertions ##

                # Create the release
                release = telescope.make_release(
                    run_id=env.dag_run.run_id,
                    data_interval_start=pendulum.parse(str(env.dag_run.data_interval_start)),
                    data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end)),
                )

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
                    telescope.cloud_workspace.project_id,
                    telescope.data_partner.bq_dataset_id,
                    telescope.data_partner.bq_table_name,
                )
                self.assert_table_integrity(table_id, expected_rows=3)
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(self.test_table, date_fields="release_date"),
                    primary_key="proprietary_id",
                )

                # Add_dataset_release_task
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
