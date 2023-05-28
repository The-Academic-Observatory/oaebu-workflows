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

# Author: Aniek Roelofs

import os
from datetime import timedelta
from unittest.mock import patch
from requests import Response

import pendulum
import vcr
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State
from click.testing import CliRunner
from croniter import croniter

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.ucl_discovery_telescope import (
    UclDiscoveryRelease,
    UclDiscoveryTelescope,
    get_downloads_per_country,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.bigquery import bq_table_id
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    random_id,
)


class TestUclDiscoveryTelescope(ObservatoryTestCase):
    """Tests for the Ucl Discovery telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests."""
        super(TestUclDiscoveryTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # Cassettes
        self.metadata_cassette = test_fixtures_folder("ucl_discovery", "metadata.yaml")
        self.country_cassette = test_fixtures_folder("ucl_discovery", "country.yaml")

    def test_dag_structure(self):
        """Test that the UCL Discovery DAG has the correct structure."""

        dag = UclDiscoveryTelescope(dag_id="Test_Dag", cloud_workspace=self.fake_cloud_workspace).make_dag()
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
        """Test that the UCL Discovery DAG can be loaded from a DAG bag."""
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="ucl_discovery",
                    name="UCL Discovery Telescope",
                    class_name="oaebu_workflows.workflows.ucl_discovery_telescope.UclDiscoveryTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )
        with env.create():
            self.assert_dag_load_from_config("ucl_discovery")

    @patch("oaebu_workflows.workflows.ucl_discovery_telescope.get_downloads_per_country")
    def test_telescope(self, mock_downloads_per_country):
        """Test the UCL Discovery telescope end to end."""
        mock_downloads_per_country.return_value = [
            {"country_code": "MX", "country_name": "Mexico", "download_count": 10},
            {"country_code": "US", "country_name": "United States", "download_count": 8},
            {"country_code": "GB", "country_name": "United Kingdom", "download_count": 6},
            {"country_code": "BR", "country_name": "Brazil", "download_count": 1},
        ], 25

        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=4, day=1)
        telescope = UclDiscoveryTelescope(
            dag_id="ucl_discovery", cloud_workspace=env.cloud_workspace, bq_dataset_id=dataset_id
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # self.setup_api()
            with env.create_dag_run(dag, execution_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)

                # Use release to check tasks
                cron_schedule = dag.normalized_schedule_interval
                cron_iter = croniter(cron_schedule, execution_date)
                end_date = pendulum.instance(cron_iter.get_next(pendulum.DateTime)) - timedelta(days=1)
                release = UclDiscoveryRelease(
                    dag_id=telescope.dag_id, run_id=env.dag_run.run_id, start_date=execution_date, end_date=end_date
                )

                # Test download
                with vcr.use_cassette(self.metadata_cassette):
                    ti = env.run_task(telescope.download.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                self.assertTrue(os.path.exists(release.download_path))
                self.assert_file_integrity(release.download_path, "8ae68aa5a455a1835fd906665746ee8c", "md5")

                # Test upload downloaded
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Test that file transformed
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertTrue(os.path.exists(release.transform_path))
                self.assert_file_integrity(release.transform_path, "ef8ba725", "gzip_crc")

                # Test that transformed file uploaded
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )

                # Test that data loaded into BigQuery
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_table_id(
                    telescope.cloud_workspace.project_id, telescope.bq_dataset_id, telescope.bq_table_name
                )
                self.assert_table_integrity(table_id, 519)

                # Add_dataset_release_task
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test that all telescope data deleted
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(release.workflow_folder)

    @patch("oaebu_workflows.workflows.ucl_discovery_telescope.retry_get_url")
    @patch("observatory.platform.airflow.Variable.get")
    def test_download(self, mock_variable_get, mock_retry_get_url):
        """Test download method of UCL Discovery release"""
        telescope = UclDiscoveryTelescope(dag_id="Test_Dag", cloud_workspace=self.fake_cloud_workspace)

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = "data"
            release = UclDiscoveryRelease(
                dag_id=telescope.dag_id,
                run_id=random_id(),
                start_date=pendulum.datetime(2020, 1, 1),
                end_date=pendulum.datetime(2020, 1, 1),
            )
            fake_response = Response()

            # test status code 200, but empty csv file
            fake_response.status_code = 200
            fake_response._content = "".encode()
            mock_retry_get_url.return_value = fake_response
            with self.assertRaises(AirflowSkipException):
                telescope.download([release])

            # test status code 200 and valid csv file
            fake_response.status_code = 200
            fake_response._content = '"eprintid","userid"\n"1234","1234"'.encode()
            mock_retry_get_url.return_value = fake_response
            telescope.download([release])
            self.assert_file_integrity(release.download_path, "13cc3a5087bbd37bf12221727bd1d93f", "md5")

    def test_get_downloads_per_country(self):
        """Test get_downloads_per_country function."""
        countries_url = (
            "https://discovery.ucl.ac.uk/cgi/stats/get?from=20210401&to=20210501&irs2report=eprint"
            "&datatype=countries&top=countries&view=Table&limit=all&set_name=eprint&export=CSV&set_value"
            "=10124354"
        )
        countries_url_empty = (
            "https://discovery.ucl.ac.uk/cgi/stats/get?from=20210401&to=20210501&irs2report=eprint"
            "&datatype=countries&top=countries&view=Table&limit=all&set_name=eprint&export=CSV"
            "&set_value=10127557"
        )
        with vcr.use_cassette(self.country_cassette):
            results, total_downloads = get_downloads_per_country(countries_url)
            self.assertListEqual(
                [
                    {"country_code": "MX", "country_name": "Mexico", "download_count": 116},
                    {"country_code": "US", "country_name": "United States", "download_count": 100},
                    {"country_code": "GB", "country_name": "United Kingdom", "download_count": 80},
                    {"country_code": "BR", "country_name": "Brazil", "download_count": 64},
                    {"country_code": "CO", "country_name": "Colombia", "download_count": 56},
                    {"country_code": "AR", "country_name": "Argentina", "download_count": 44},
                    {"country_code": "DE", "country_name": "Germany", "download_count": 35},
                    {"country_code": "CL", "country_name": "Chile", "download_count": 27},
                    {"country_code": "EC", "country_name": "Ecuador", "download_count": 26},
                    {"country_code": "CA", "country_name": "Canada", "download_count": 16},
                    {"country_code": "NL", "country_name": "Netherlands", "download_count": 15},
                    {"country_code": "FR", "country_name": "France", "download_count": 15},
                    {"country_code": "ES", "country_name": "Spain", "download_count": 11},
                    {"country_code": "IT", "country_name": "Italy", "download_count": 8},
                    {"country_code": "PT", "country_name": "Portugal", "download_count": 6},
                    {"country_code": "TW", "country_name": "Taiwan", "download_count": 6},
                    {"country_code": "SE", "country_name": "Sweden", "download_count": 6},
                    {"country_code": "PE", "country_name": "Peru", "download_count": 5},
                    {"country_code": "NO", "country_name": "Norway", "download_count": 5},
                    {"country_code": "CH", "country_name": "Switzerland", "download_count": 4},
                    {"country_code": "CR", "country_name": "Costa Rica", "download_count": 4},
                    {"country_code": "GR", "country_name": "Greece", "download_count": 4},
                    {"country_code": "AT", "country_name": "Austria", "download_count": 4},
                    {"country_code": "CZ", "country_name": "Czech Republic", "download_count": 3},
                    {"country_code": "AU", "country_name": "Australia", "download_count": 3},
                    {"country_code": "IE", "country_name": "Ireland", "download_count": 3},
                    {"country_code": "PH", "country_name": "Philippines", "download_count": 3},
                    {"country_code": "IN", "country_name": "India", "download_count": 2},
                    {"country_code": "NZ", "country_name": "New Zealand", "download_count": 2},
                    {"country_code": "PR", "country_name": "Puerto Rico", "download_count": 2},
                    {"country_code": "FI", "country_name": "Finland", "download_count": 2},
                    {"country_code": "ZA", "country_name": "South Africa", "download_count": 2},
                    {"country_code": "RO", "country_name": "Romania", "download_count": 2},
                    {"country_code": "VE", "country_name": "Venezuela", "download_count": 1},
                    {"country_code": "EU", "country_name": "Europe", "download_count": 1},
                    {"country_code": "RS", "country_name": "Serbia", "download_count": 1},
                    {"country_code": "NG", "country_name": "Nigeria", "download_count": 1},
                    {"country_code": "HR", "country_name": "Croatia", "download_count": 1},
                    {"country_code": "BE", "country_name": "Belgium", "download_count": 1},
                    {"country_code": "MM", "country_name": "Myanmar", "download_count": 1},
                    {"country_code": "DK", "country_name": "Denmark", "download_count": 1},
                    {"country_code": "ID", "country_name": "Indonesia", "download_count": 1},
                    {"country_code": "JP", "country_name": "Japan", "download_count": 1},
                    {"country_code": "SK", "country_name": "Slovakia", "download_count": 1},
                ],
                results,
            )
            self.assertEqual(692, total_downloads)
            results, total_downloads = get_downloads_per_country(countries_url_empty)
            self.assertEqual([], results)
            self.assertEqual(0, total_downloads)
