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

# Author: Aniek Roelofs, Keegan Smith

import gzip
import json
import os
from unittest.mock import patch

import pendulum
from airflow.models import Connection
from airflow.utils.state import State
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from oaebu_workflows.google_analytics3_telescope.google_analytics3_telescope import GoogleAnalytics3Telescope
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.config import test_fixtures_folder
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.bigquery import bq_table_id
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    load_and_parse_json,
)


class TestGoogleAnalytics3Telescope(ObservatoryTestCase):
    """Tests for the Google Analytics telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestGoogleAnalytics3Telescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.view_id = "11235141"
        self.pagepath_regex = r".*regex$"
        self.organisation_name = "UCL Press"

        fixtures_folder = test_fixtures_folder(workflow_module="google_analytics3_telescope")
        self.test_table = os.path.join(fixtures_folder, "test_table.json")
        self.test_table_anu = os.path.join(fixtures_folder, "test_table_anu.json")

    def test_dag_structure(self):
        """Test that the Google Analytics DAG has the correct structure.
        :return: None
        """
        cloud_workspace = self.fake_cloud_workspace
        dag = GoogleAnalytics3Telescope(
            dag_id="google_analytics_test",
            organisation_name="Organisation Name",
            cloud_workspace=cloud_workspace,
            view_id=self.view_id,
            pagepath_regex=self.pagepath_regex,
        ).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download_transform"],
                "download_transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Google Analytics DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="google_analytics3",
                    name="My Google Analytics Workflow",
                    class_name="oaebu_workflows.google_analytics3_telescope.google_analytics3_telescope.GoogleAnalytics3Telescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(organisation_name="My Organisation", pagepath_regex="", view_id="123456"),
                )
            ]
        )
        with env.create():
            self.assert_dag_load_from_config("google_analytics3")

        # Errors should be raised if kwargs dict not supplied
        env.workflows[0].kwargs = {}
        with env.create():
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load_from_config("google_analytics3")
            msg = cm.exception.args[0]
            self.assertTrue("missing 3 required positional arguments" in msg)
            self.assertTrue("organisation_name" in msg)
            self.assertTrue("pagepath_regex" in msg)
            self.assertTrue("view_id" in msg)

    @patch("oaebu_workflows.google_analytics3_telescope.google_analytics3_telescope.build")
    @patch(
        "oaebu_workflows.google_analytics3_telescope.google_analytics3_telescope.ServiceAccountCredentials"
    )
    def test_telescope(self, mock_account_credentials, mock_build):
        """Test the Google Analytics telescope end to end specifically for ANU Press, to test custom dimensions.
        :return: None.
        """
        # Mock the Google Reporting Analytics API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(create_http_mock_sequence(GoogleAnalytics3Telescope.ANU_ORG_NAME))
        mock_build.return_value = build("analyticsreporting", "v4", http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        # Setup Telescope
        execution_date = pendulum.datetime(year=2022, month=6, day=1)
        partner = partner_from_str("google_analytics3")
        partner.bq_dataset_id = env.add_dataset()
        telescope = GoogleAnalytics3Telescope(
            dag_id="google_analytics_test",
            organisation_name=GoogleAnalytics3Telescope.ANU_ORG_NAME,
            cloud_workspace=env.cloud_workspace,
            view_id=self.view_id,
            pagepath_regex=self.pagepath_regex,
            data_partner=partner,
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            with env.create_dag_run(dag, execution_date):
                # Add OAEBU service account connection connection
                conn = Connection(
                    conn_id="oaebu_service_account",
                    uri=f"google-cloud-platform://?type=service_account&private_key_id=private_key_id"
                    f"&private_key=private_key"
                    f"&client_email=client_email"
                    f"&client_id=client_id",
                )
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test download_transform task
                ti = env.run_task(telescope.download_transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test that transformed file uploaded
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test that data loaded into BigQuery
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Use release to check tasks
                release = telescope.make_release(
                    run_id=env.dag_run.run_id,
                    data_interval_start=pendulum.parse(str(env.dag_run.data_interval_start)),
                    data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end)),
                )[0]

                # Test download_transform task
                self.assertTrue(os.path.exists(release.transform_path))
                self.assertTrue(os.path.isfile(release.transform_path))
                # Use frozenset to test results are as expected, many dict transformations re-order items in dict
                actual_list = []
                with gzip.open(release.transform_path, "rb") as f:
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

                # Test that data loaded into BigQuery
                table_id = bq_table_id(
                    telescope.cloud_workspace.project_id,
                    telescope.data_partner.bq_dataset_id,
                    telescope.data_partner.bq_table_name,
                )
                self.assert_table_integrity(table_id, expected_rows=3)
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(self.test_table_anu, date_fields=["release_date", "start_date", "end_date"]),
                    primary_key="url",
                )

                # add_dataset_release_task
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
    if organisation_name == GoogleAnalytics3Telescope.ANU_ORG_NAME:
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
    if organisation_name == GoogleAnalytics3Telescope.ANU_ORG_NAME:
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
