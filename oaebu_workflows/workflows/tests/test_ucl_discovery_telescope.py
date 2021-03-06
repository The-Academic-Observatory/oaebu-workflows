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

import pendulum
import vcr
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.connection import Connection
from click.testing import CliRunner
from croniter import croniter
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from oaebu_workflows.workflows.ucl_discovery_telescope import (
    UclDiscoveryRelease,
    UclDiscoveryTelescope,
    get_downloads_per_country,
)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
)
from requests.exceptions import RetryError
from oaebu_workflows.config import test_fixtures_folder
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection
from airflow.utils.state import State


class TestUclDiscoveryTelescope(ObservatoryTestCase):
    """Tests for the Ucl Discovery telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestUclDiscoveryTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.organisation_name = "ucl_press"
        self.host = "localhost"
        self.api_port = find_free_port()

        self.metadata_cassette = test_fixtures_folder("ucl_discovery", "metadata.yaml")
        self.country_cassette = test_fixtures_folder("ucl_discovery", "country.yaml")
        self.download_hash = "8ae68aa5a455a1835fd906665746ee8c"
        self.transform_hash = "5a552603"

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "UCL Press"

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Ucl Discovery Telescope"
        workflow_type = WorkflowType(name=name, type_id=UclDiscoveryTelescope.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name=self.org_name,
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=UclDiscoveryTelescope.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Ucl Discovery Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the UCL Discovery DAG has the correct structure.
        :return: None
        """
        organisation = Organisation(name=self.organisation_name)
        dag = UclDiscoveryTelescope(organisation).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the UCL Discovery DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "ucl_discovery_telescope.py")
            self.assert_dag_load("ucl_discovery_ucl_press", dag_file)

    @patch("oaebu_workflows.workflows.ucl_discovery_telescope.get_downloads_per_country")
    def test_telescope(self, mock_downloads_per_country):
        """Test the UCL Discovery telescope end to end.
        :return: None.
        """
        mock_downloads_per_country.return_value = [
            {"country_code": "MX", "country_name": "Mexico", "download_count": 10},
            {"country_code": "US", "country_name": "United States", "download_count": 8},
            {"country_code": "GB", "country_name": "United Kingdom", "download_count": 6},
            {"country_code": "BR", "country_name": "Brazil", "download_count": 1},
        ], 25

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=4, day=1)
        organisation = Organisation(
            name=self.organisation_name,
            project_id=self.project_id,
            download_bucket=env.download_bucket,
            transform_bucket=env.transform_bucket,
        )
        telescope = UclDiscoveryTelescope(organisation=organisation, dataset_id=dataset_id, workflow_id=1)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            with env.create_dag_run(dag, execution_date):
                # Add OAEBU service account connection connection
                conn = Connection(
                    conn_id=AirflowConns.OAEBU_SERVICE_ACCOUNT,
                    uri=f"google-cloud-platform://?type=service_account&private_key_id=private_key_id"
                    f"&private_key=private_key"
                    f"&client_email=client_email"
                    f"&client_id=client_id",
                )
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Use release to check tasks
                cron_schedule = dag.normalized_schedule_interval
                cron_iter = croniter(cron_schedule, execution_date)
                end_date = pendulum.instance(cron_iter.get_next(pendulum.DateTime)) - timedelta(days=1)
                release = UclDiscoveryRelease(telescope.dag_id, execution_date, end_date, organisation)

                # Test download
                with vcr.use_cassette(self.metadata_cassette):
                    env.run_task(telescope.download.__name__)
                self.assertEqual(1, len(release.download_files))
                for file in release.download_files:
                    self.assert_file_integrity(file, self.download_hash, "md5")

                # Test upload downloaded
                env.run_task(telescope.upload_downloaded.__name__)
                for file in release.download_files:
                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(1, len(release.transform_files))
                for file in release.transform_files:
                    self.assert_file_integrity(file, self.transform_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load_partition.__name__)
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                    expected_rows = 519
                    self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    @patch("oaebu_workflows.workflows.ucl_discovery_telescope.retry_session")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_download(self, mock_variable_get, mock_retry_session):
        """Test download method of UCL Discovery release

        :param mock_variable_get: Mock Variable get
        :param mock_retry_session: Mock retry_session
        :return: None.
        """
        organisation = Organisation(
            name=self.organisation_name,
            project_id=self.project_id,
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        telescope = UclDiscoveryTelescope(organisation=organisation, dataset_id="dataset_id")
        release = UclDiscoveryRelease(
            telescope.dag_id, pendulum.datetime(2020, 1, 1), pendulum.datetime(2020, 1, 1), organisation
        )

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = "data"

            # test status code is not 200
            mock_retry_session().get.return_value.status_code = 400
            with self.assertRaises(AirflowException):
                release.download()

            # test status code 200, but empty csv file
            mock_retry_session().get.return_value.status_code = 200
            mock_retry_session().get.return_value.content = "".encode()
            with self.assertRaises(AirflowSkipException):
                release.download()

            # test status code 200 and valid csv file
            mock_retry_session().get.return_value.content = '"eprintid","userid"\n"1234","1234"'.encode()
            release.download()
            self.assert_file_integrity(release.download_path, "13cc3a5087bbd37bf12221727bd1d93f", "md5")

            # test retry error
            mock_retry_session.side_effect = RetryError()
            with self.assertRaises(RetryError):
                release.download()

    def test_get_downloads_per_country(self):
        """Test get_downloads_per_country function.

        :return: None.
        """
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
