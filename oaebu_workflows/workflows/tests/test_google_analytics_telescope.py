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

import gzip
import json
import os
from datetime import datetime, timedelta
from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection
from croniter import croniter
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence
from observatory.api.client.model.organisation import Organisation
from oaebu_workflows.workflows.google_analytics_telescope import (
    GoogleAnalyticsRelease,
    GoogleAnalyticsTelescope,
)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port
)
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


class TestGoogleAnalyticsTelescope(ObservatoryTestCase):
    """Tests for the Google Analytics telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestGoogleAnalyticsTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "UCL Press"

    def setup_api(self, org_name=None):
        dt = pendulum.now("UTC")

        name = "Google Analytics Telescope"
        workflow_type = WorkflowType(name=name, type_id=GoogleAnalyticsTelescope.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        org_name_ = self.org_name if org_name is None else org_name

        organisation = Organisation(
            name=org_name_,
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
            type_id=GoogleAnalyticsTelescope.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Google Analytics Dataset",
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
        """Test that the Google Analytics DAG has the correct structure.
        :return: None
        """
        organisation = Organisation(name="Organisation Name")
        dag = GoogleAnalyticsTelescope(organisation, "1234", r"regex").make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download_transform"],
                "download_transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Google Analytics DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "google_analytics_telescope.py")
            self.assert_dag_load("google_analytics_ucl_press", dag_file)

    @patch("oaebu_workflows.workflows.google_analytics_telescope.build")
    @patch("oaebu_workflows.workflows.google_analytics_telescope.ServiceAccountCredentials")
    def test_telescope(self, mock_account_credentials, mock_build):
        """Test the Google Analytics telescope end to end.
        :return: None.
        """
        # Set up organisation name and telescope extra values
        self.organisation_name = "UCL Press"
        self.extra = {"view_id": "11235141", "pagepath_regex": r".*regex$"}
        self.view_id = self.extra.get("view_id")
        self.pagepath_regex = self.extra.get("pagepath_regex")

        # Mock the Google Reporting Analytics API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(create_http_mock_sequence(self.organisation_name))
        mock_build.return_value = build("analyticsreporting", "v4", http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2022, month=6, day=1)
        organisation = Organisation(
            name=self.organisation_name,
            project_id=self.project_id,
            download_bucket=env.download_bucket,
            transform_bucket=env.transform_bucket,
        )
        telescope = GoogleAnalyticsTelescope(
            organisation=organisation,
            view_id=self.view_id,
            pagepath_regex=self.pagepath_regex,
            dataset_id=dataset_id,
            workflow_id=1,
        )
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
                end_date = pendulum.instance(cron_iter.get_next(datetime)) - timedelta(days=1)
                release = GoogleAnalyticsRelease(telescope.dag_id, execution_date, end_date, organisation)

                # Test download_transform task
                env.run_task(telescope.download_transform.__name__)
                self.assertEqual(1, len(release.transform_files))
                for file in release.transform_files:
                    self.assertTrue(os.path.isfile(file))
                    # Use frozenset to test results are as expected, many dict transformations re-order items in dict
                    actual_list = []
                    with gzip.open(file, "rb") as f:
                        for line in f:
                            actual_list.append(json.loads(line))
                    expected_list = [
                        {
                            "url": "/base/path/151420",
                            "title": "Anything public program drive north.",
                            "start_date": "2022-06-01",
                            "end_date": "2022-06-30",
                            "average_time": 59.5,
                            "unique_views": {
                                "country": [{"name": "country 1", "value": 3}, {"name": "country 2", "value": 3}],
                                "referrer": [{"name": "referrer 1", "value": 3}, {"name": "referrer 2", "value": 3}],
                                "social_network": [
                                    {"name": "social_network 1", "value": 3},
                                    {"name": "social_network 2", "value": 3},
                                ],
                            },
                            "page_views": {
                                "country": [{"name": "country 1", "value": 4}, {"name": "country 2", "value": 4}],
                                "referrer": [{"name": "referrer 1", "value": 4}, {"name": "referrer 2", "value": 4}],
                                "social_network": [
                                    {"name": "social_network 1", "value": 4},
                                    {"name": "social_network 2", "value": 4},
                                ],
                            },
                            "sessions": {
                                "country": [{"name": "country 1", "value": 1}, {"name": "country 2", "value": 1}],
                                "source": [{"name": "source 1", "value": 1}, {"name": "source 2", "value": 1}],
                            },
                            "release_date": "2022-06-30",
                        },
                        {
                            "url": "/base/path/833557",
                            "title": "Standard current never no.",
                            "start_date": "2022-06-01",
                            "end_date": "2022-06-30",
                            "average_time": 49.6,
                            "unique_views": {"country": [], "referrer": [], "social_network": []},
                            "page_views": {"country": [], "referrer": [], "social_network": []},
                            "sessions": {"country": [], "source": []},
                            "release_date": "2022-06-30",
                        },
                        {
                            "url": "/base/path/833557?fbclid=123",
                            "title": "Standard current never no.",
                            "start_date": "2022-06-01",
                            "end_date": "2022-06-30",
                            "average_time": 38.8,
                            "unique_views": {
                                "country": [{"name": "country 2", "value": 2}],
                                "referrer": [{"name": "referrer 2", "value": 2}],
                                "social_network": [{"name": "social_network 2", "value": 2}],
                            },
                            "page_views": {
                                "country": [{"name": "country 2", "value": 4}],
                                "referrer": [{"name": "referrer 2", "value": 4}],
                                "social_network": [{"name": "social_network 2", "value": 4}],
                            },
                            "sessions": {"country": [], "source": []},
                            "release_date": "2022-06-30",
                        },
                    ]
                    self.assertEqual(3, len(actual_list))
                    self.assertEqual(frozenset(expected_list[0]), frozenset(actual_list[0]))
                    self.assertEqual(frozenset(expected_list[1]), frozenset(actual_list[1]))
                    self.assertEqual(frozenset(expected_list[2]), frozenset(actual_list[2]))

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load_partition.__name__)
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                    expected_rows = 3
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

    @patch("oaebu_workflows.workflows.google_analytics_telescope.build")
    @patch("oaebu_workflows.workflows.google_analytics_telescope.ServiceAccountCredentials")
    def test_telescope_anu(self, mock_account_credentials, mock_build):
        """Test the Google Analytics telescope end to end specifically for ANU Press, to test custom dimensions.
        :return: None.
        """
        # Set up organisation name and telescope extra values
        self.organisation_name = GoogleAnalyticsTelescope.ANU_ORG_NAME
        self.extra = {"view_id": "12354151", "pagepath_regex": ""}
        self.view_id = self.extra.get("view_id")
        self.pagepath_regex = self.extra.get("pagepath_regex")

        # Mock the Google Reporting Analytics API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(create_http_mock_sequence(self.organisation_name))
        mock_build.return_value = build("analyticsreporting", "v4", http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2022, month=6, day=1)
        organisation = Organisation(
            name=self.organisation_name,
            project_id=self.project_id,
            download_bucket=env.download_bucket,
            transform_bucket=env.transform_bucket,
        )
        telescope = GoogleAnalyticsTelescope(
            organisation=organisation,
            view_id=self.view_id,
            pagepath_regex=self.pagepath_regex,
            dataset_id=dataset_id,
            workflow_id=1,
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_connections(env)
            self.setup_api(org_name=self.organisation_name)
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
                end_date = pendulum.instance(cron_iter.get_next(datetime)) - timedelta(days=1)
                release = GoogleAnalyticsRelease(telescope.dag_id, execution_date, end_date, organisation)

                # Test download_transform task
                env.run_task(telescope.download_transform.__name__)
                self.assertEqual(1, len(release.transform_files))
                for file in release.transform_files:
                    self.assertTrue(os.path.isfile(file))
                    # Use frozenset to test results are as expected, many dict transformations re-order items in dict
                    actual_list = []
                    with gzip.open(file, "rb") as f:
                        for line in f:
                            actual_list.append(json.loads(line))
                    expected_list = [
                        {
                            "url": "/base/path/151420",
                            "title": "Anything public program drive north.",
                            "start_date": "2022-06-01",
                            "end_date": "2022-06-30",
                            "average_time": 59.5,
                            "unique_views": {
                                "country": [{"name": "country 1", "value": 3}, {"name": "country 2", "value": 3}],
                                "referrer": [{"name": "referrer 1", "value": 3}, {"name": "referrer 2", "value": 3}],
                                "social_network": [
                                    {"name": "social_network 1", "value": 3},
                                    {"name": "social_network 2", "value": 3},
                                ],
                            },
                            "page_views": {
                                "country": [{"name": "country 1", "value": 4}, {"name": "country 2", "value": 4}],
                                "referrer": [{"name": "referrer 1", "value": 4}, {"name": "referrer 2", "value": 4}],
                                "social_network": [
                                    {"name": "social_network 1", "value": 4},
                                    {"name": "social_network 2", "value": 4},
                                ],
                            },
                            "sessions": {
                                "country": [{"name": "country 1", "value": 1}, {"name": "country 2", "value": 1}],
                                "source": [{"name": "source 1", "value": 1}, {"name": "source 2", "value": 1}],
                            },
                            "publication_id": "1234567890123",
                            "publication_type": "book",
                            "publication_imprint": "imprint",
                            "publication_group": "group",
                            "publication_whole_or_part": "whole",
                            "publication_format": "PDF",
                            "release_date": "2022-06-30",
                        },
                        {
                            "url": "/base/path/833557",
                            "title": "Standard current never no.",
                            "start_date": "2022-06-01",
                            "end_date": "2022-06-30",
                            "average_time": 49.6,
                            "unique_views": {"country": [], "referrer": [], "social_network": []},
                            "page_views": {"country": [], "referrer": [], "social_network": []},
                            "sessions": {"country": [], "source": []},
                            "publication_id": "1234567891234",
                            "publication_type": "book",
                            "publication_imprint": "imprint",
                            "publication_group": "(none)",
                            "publication_whole_or_part": "part",
                            "publication_format": "HTML",
                            "release_date": "2022-06-30",
                        },
                        {
                            "url": "/base/path/833557?fbclid=123",
                            "title": "Standard current never no.",
                            "start_date": "2022-06-01",
                            "end_date": "2022-06-30",
                            "average_time": 38.8,
                            "unique_views": {
                                "country": [{"name": "country 2", "value": 2}],
                                "referrer": [{"name": "referrer 2", "value": 2}],
                                "social_network": [{"name": "social_network 2", "value": 2}],
                            },
                            "page_views": {
                                "country": [{"name": "country 2", "value": 4}],
                                "referrer": [{"name": "referrer 2", "value": 4}],
                                "social_network": [{"name": "social_network 2", "value": 4}],
                            },
                            "sessions": {"country": [], "source": []},
                            "publication_id": "1234567891234",
                            "publication_type": "book",
                            "publication_imprint": "imprint",
                            "publication_group": "(none)",
                            "publication_whole_or_part": "part",
                            "publication_format": "HTML",
                            "release_date": "2022-06-30",
                        },
                    ]
                    self.assertEqual(3, len(actual_list))
                    self.assertEqual(frozenset(expected_list[0]), frozenset(actual_list[0]))
                    self.assertEqual(frozenset(expected_list[1]), frozenset(actual_list[1]))
                    self.assertEqual(frozenset(expected_list[2]), frozenset(actual_list[2]))

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load_partition.__name__)
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                    expected_rows = 3
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


def create_http_mock_sequence(organisation_name: str) -> list:
    """Create a list of http mock sequences for listing books and getting dimension data

    :param organisation_name: The organisation name (add custom dimensions for ANU)
    :return: A list with HttpMockSequence instances
    """
    http_mock_sequence = []
    list_books = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:pagepath", "ga:pageTitle"],
                    "metricHeader": {"metricHeaderEntries": [{"name": "ga:avgTimeOnPage", "type": "TIME"}]},
                },
                "data": {
                    "rows": [
                        {
                            "dimensions": ["/base/path/151420", "Anything public program drive north."],
                            "metrics": [{"values": ["59.5"]}],
                        },
                        {
                            "dimensions": ["/base/path/833557", "Standard current never no."],
                            "metrics": [{"values": ["49.6"]}],
                        },
                    ],
                    "totals": [{"values": ["109.1"]}],
                    "rowCount": 2,
                    "minimums": [{"values": ["49.6"]}],
                    "maximums": [{"values": ["59.5"]}],
                    "isDataGolden": True,
                },
                "nextPageToken": "200",
            }
        ]
    }
    # Add custom dimensions from ANU Press
    if organisation_name == GoogleAnalyticsTelescope.ANU_ORG_NAME:
        list_books["reports"][0]["columnHeader"]["dimensions"] += [f"ga:dimension{(str(i))}" for i in range(1, 7)]
        list_books["reports"][0]["data"]["rows"][0]["dimensions"] += [
            "1234567890123",
            "book",
            "imprint",
            "group",
            "whole",
            "PDF",
        ]
        list_books["reports"][0]["data"]["rows"][1]["dimensions"] += [
            "1234567891234",
            "book",
            "imprint",
            "(none)",
            "part",
            "HTML",
        ]
    list_books_next_page = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:pagepath", "ga:pageTitle"],
                    "metricHeader": {"metricHeaderEntries": [{"name": "ga:avgTimeOnPage", "type": "TIME"}]},
                },
                "data": {
                    "rows": [
                        {
                            "dimensions": ["/base/path/833557?fbclid=123", "Standard current never no."],
                            "metrics": [{"values": ["38.8"]}],
                        }
                    ],
                    "totals": [{"values": ["38.8"]}],
                    "rowCount": 1,
                    "minimums": [{"values": ["38.8"]}],
                    "maximums": [{"values": ["38.8"]}],
                    "isDataGolden": True,
                },
            }
        ]
    }
    # Add custom dimensions from ANU Press
    if organisation_name == GoogleAnalyticsTelescope.ANU_ORG_NAME:
        list_books_next_page["reports"][0]["columnHeader"]["dimensions"] += [
            f"ga:dimension{(str(i))}" for i in range(1, 7)
        ]
        list_books_next_page["reports"][0]["data"]["rows"][0]["dimensions"] += [
            "1234567891234",
            "book",
            "imprint",
            "(none)",
            "part",
            "HTML",
        ]
    http_mock_sequence.append(({"status": "200"}, json.dumps(list_books)))
    http_mock_sequence.append(({"status": "200"}, json.dumps(list_books_next_page)))
    for dimension in ["country", "referrer", "social_network", "source"]:
        results = {
            "reports": [
                {
                    "columnHeader": {
                        "dimensions": ["ga:pagePath", "ga:country"],
                        "metricHeader": {
                            "metricHeaderEntries": [
                                {"name": "ga:uniquePageviews", "type": "INTEGER"},
                                {"name": "ga:Pageviews", "type": "INTEGER"},
                                {"name": "ga:sessions", "type": "INTEGER"},
                            ]
                        },
                    },
                    "data": {
                        "rows": [
                            {
                                "dimensions": ["/base/path/151420", dimension + " 1"],
                                "metrics": [{"values": ["3", "4", "1"]}],
                            },
                            {
                                "dimensions": ["/base/path/151420", dimension + " 2"],
                                "metrics": [{"values": ["3", "4", "1"]}],
                            },
                            {
                                "dimensions": ["/base/path/833557", dimension + " 1"],
                                "metrics": [{"values": ["0", "0", "0"]}],  # Added a zero case for code coverage
                            },
                            {
                                "dimensions": ["/base/path/833557?fbclid=123", dimension + " 2"],
                                "metrics": [{"values": ["2", "4", "0"]}],
                            },
                        ],
                        "totals": [{"values": ["6", "9", "1"]}],
                        "rowCount": 3,
                        "minimums": [{"values": ["1", "3", "0"]}],
                        "maximums": [{"values": ["3", "4", "1"]}],
                        "isDataGolden": True,
                    },
                }
            ]
        }
        http_mock_sequence.append(({"status": "200"}, json.dumps(results)))

    return http_mock_sequence
