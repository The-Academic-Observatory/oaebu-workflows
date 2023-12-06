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

import os
from unittest import TestCase
from unittest.mock import patch, call

import pendulum
from airflow.utils.state import State
from airflow.models.connection import Connection
import vcr

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope import (
    UclDiscoveryTelescope,
    get_isbn_eprint_mappings,
    download_discovery_stats,
    transform_discovery_stats,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.bigquery import bq_table_id
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    load_and_parse_json,
)


class TestUclDiscoveryTelescope(ObservatoryTestCase):
    """Tests for the Ucl Discovery telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests."""
        super(TestUclDiscoveryTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        fixtures_folder = test_fixtures_folder(workflow_module="ucl_discovery_telescope")
        self.download_cassette = os.path.join(fixtures_folder, "download_cassette.yaml")
        self.test_table = os.path.join(fixtures_folder, "test_table.json")

    def test_dag_structure(self):
        """Test that the UCL Discovery DAG has the correct structure."""

        dag = UclDiscoveryTelescope(
            dag_id="Test_Dag", cloud_workspace=self.fake_cloud_workspace, sheet_id="foo"
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
        """Test that the UCL Discovery DAG can be loaded from a DAG bag."""
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="ucl_discovery",
                    name="UCL Discovery Telescope",
                    class_name="oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.UclDiscoveryTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(sheet_id="foo"),
                )
            ]
        )
        with env.create():
            self.assert_dag_load_from_config("ucl_discovery")

    def test_telescope(self):
        """Test the UCL Discovery telescope end to end."""
        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )

        # Setup Telescope
        data_partner = partner_from_str("ucl_discovery")
        data_partner.bq_dataset_id = env.add_dataset()
        telescope = UclDiscoveryTelescope(
            dag_id="ucl_discovery",
            cloud_workspace=env.cloud_workspace,
            sheet_id="foo",
            data_partner=data_partner,
            max_threads=1,
        )
        dag = telescope.make_dag()
        execution_date = pendulum.datetime(year=2023, month=6, day=1)

        # Create the Observatory environment and run tests
        with env.create(), env.create_dag_run(dag, execution_date):
            # env.add_connection(Connection(conn_id=telescope.oaebu_service_account_conn_id))
            # Mock return values of download function
            interval_start = pendulum.instance(env.dag_run.data_interval_start)
            sheet_return = [
                ["ISBN13", "discovery_eprintid", "date", "title_list_title"],
                ["ISBN_1", "eprint_id1", interval_start.add(days=10).format("YYYYMMDD"), "title1"],
                ["ISBN_2", "", interval_start.add(days=10).format("YYYYMMDD"), "title2"],  # should be ignored
                ["ISBN_3", "eprint_id3", interval_start.add(years=1).format("YYYYMMDD"), "title3"],  # should be ignored
                ["", "eprint_id4", interval_start.add(days=10).format("YYYYMMDD"), "title4"],  # should be ignored
                ["ISBN_5", "eprint_id5", interval_start.subtract(months=5).format("YYYYMMDD"), "title5"],
            ]
            conn = Connection(
                conn_id="oaebu_service_account",
                uri=f"google-cloud-platform://?type=service_account&private_key_id=private_key_id"
                f"&private_key=private_key"
                f"&client_email=client_email"
                f"&client_id=client_id"
                f"&token_uri=token_uri",
            )
            env.add_connection(conn)

            ############################
            ### Main telescope tasks ###
            ############################

            # Test that all dependencies are specified: no error should be thrown
            ti = env.run_task(telescope.check_dependencies.__name__)

            # download
            cassette = vcr.VCR(record_mode="none")
            sa_patch = patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.service_account")
            conn_patch = patch(
                "oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.BaseHook.get_connection"
            )
            build_patch = patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.discovery.build")
            with sa_patch, conn_patch, build_patch as mock_build, cassette.use_cassette(self.download_cassette):
                mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
                mock_service.execute.return_value = {"values": sheet_return}
                ti = env.run_task(telescope.download.__name__)
            self.assertEqual(ti.state, State.SUCCESS)

            # upload_downloaded
            ti = env.run_task(telescope.upload_downloaded.__name__)
            self.assertEqual(ti.state, State.SUCCESS)

            # transform
            with sa_patch, conn_patch, build_patch as mock_build:
                mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
                mock_service.execute.return_value = {"values": sheet_return}
                ti = env.run_task(telescope.transform.__name__)
            self.assertEqual(ti.state, State.SUCCESS)

            # upload_transformed
            ti = env.run_task(telescope.upload_transformed.__name__)
            self.assertEqual(ti.state, State.SUCCESS)

            # bq_load
            ti = env.run_task(telescope.bq_load.__name__)
            self.assertEqual(ti.state, State.SUCCESS)

            ##############################################
            ### Create the release and make assertions ###
            ##############################################

            release = telescope.make_release(
                run_id=env.dag_run.run_id,
                data_interval_start=pendulum.parse(str(env.dag_run.data_interval_start)),
                data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end)),
            )

            # Download
            self.assertTrue(os.path.exists(release.download_country_path))
            self.assertTrue(os.path.exists(release.download_totals_path))

            # Upload Downloaded
            download_country_blob = gcs_blob_name_from_path(release.download_country_path)
            self.assert_blob_integrity(env.download_bucket, download_country_blob, release.download_country_path)
            download_totals_blob = gcs_blob_name_from_path(release.download_totals_path)
            self.assert_blob_integrity(env.download_bucket, download_totals_blob, release.download_totals_path)

            # Transform
            self.assertTrue(os.path.exists(release.transform_path))

            # Upload Transform
            self.assert_blob_integrity(
                env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
            )

            # Bigquery load
            table_id = bq_table_id(
                telescope.cloud_workspace.project_id,
                telescope.data_partner.bq_dataset_id,
                telescope.data_partner.bq_table_name,
            )
            self.assert_table_integrity(table_id, 2)
            self.assert_table_content(
                table_id, load_and_parse_json(self.test_table, date_fields="release_date"), "ISBN"
            )

            ###################
            ### Final tasks ###
            ###################

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


class TestGetIsbnEprintMappings(TestCase):
    """Tests for the get_isbn_eprint_mappings function"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Set the cutoff date for the tests
        self.cutoff_date = pendulum.datetime(year=2023, month=6, day=30)

    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.service_account")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.discovery.build")
    def test_get_isbn_eprint_mappings(self, mock_build, mock_get_connection, mock_sa):
        # Mock the Google Sheets API response
        sheet_contents = [
            ["ISBN13", "discovery_eprintid", "date", "title_list_title"],
            ["111", "eprint_1", "2023-08-01", "title1"],  # past cutoff, should be ignored
            ["222", "eprint_2", "2023-06-01", "title2"],
            ["333", "eprint_3", "2023-07-01", "title3"],  # past cutoff, should be ignored
            ["444", "eprint_4", "2023-06-30", "title4"],
        ]
        mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
        mock_service.execute.return_value = {"values": sheet_contents}

        # Call the function to test
        mappings = get_isbn_eprint_mappings("sheet_id", "service_account_conn_id", self.cutoff_date)

        # Assert that the returned mappings match the expected mappings
        expected_mappings = {
            "eprint_2": {"ISBN13": "222", "title": "title2"},
            "eprint_4": {"ISBN13": "444", "title": "title4"},
        }
        self.assertEqual(mappings, expected_mappings)

    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.service_account")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.discovery.build")
    def test_invalid_header(self, mock_build, mock_get_connection, mock_sa):
        # Mock the Google Sheets API response with an invalid header
        invalid_sheet_contents = [
            [
                "ISBN13",
                "discovery_id",
                "date",
                "title_list_title",
            ],  # Invalid header, should contain "discovery_eprintid"
            ["222", "eprint_2", "2023-06-01", "title2"],
        ]
        mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
        mock_service.execute.return_value = {"values": invalid_sheet_contents}

        with self.assertRaisesRegex(ValueError, "Invalid header found"):
            get_isbn_eprint_mappings("sheet_id", "service_account_conn_id", self.cutoff_date)

    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.service_account")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.discovery.build")
    def test_empty_sheet(self, mock_build, mock_get_connection, mock_sa):
        # Mock the Google Sheets API response with an empty sheet
        mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {}

        with self.assertRaisesRegex(ValueError, "No content found"):
            get_isbn_eprint_mappings("sheet_id", "service_account_conn_id", self.cutoff_date)

    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.service_account")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.discovery.build")
    def test_missing_values(self, mock_build, mock_get_connection, mock_sa):
        # Mock the Google Sheets API response with a missing value
        sheet_contents = [
            ["ISBN13", "discovery_eprintid", "date", "title_list_title"],
            ["111", "", "2023-06-01", "title1"],  # eprint ID missing
            ["", "eprint_2", "2023-06-01", "title2"],  # ISBN missing
            ["333", "eprint_3", "2023-06-01", ""],  # Title missing, should still pass
        ]
        mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
        mock_service.execute.return_value = {"values": sheet_contents}

        mappings = get_isbn_eprint_mappings("sheet_id", "service_account_conn_id", self.cutoff_date)
        expected_mappings = {"eprint_3": {"ISBN13": "333", "title": ""}}
        self.assertEqual(mappings, expected_mappings)


class TestDownloadDiscoveryStats(TestCase):
    """Tests for the download_discovery_stats function"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Set the cutoff date for the tests
        self.start_date = pendulum.datetime(2022, 1, 1)
        self.end_date = pendulum.datetime(2022, 1, 31)
        self.start_formatted = self.start_date.format("YYYYMMDD")
        self.end_formatted = self.end_date.format("YYYYMMDD")
        self.eprint_id = "12345"

    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.retry_get_url")
    def test_download_discovery_stats(self, mock_retry_get_url):
        """Test the download_discovery_stats function works with correct inputs"""
        expected_countries_url = (
            "https://discovery.ucl.ac.uk/cgi/stats/get"
            f"?from={self.start_date.format('YYYYMMDD')}&to={self.end_date.format('YYYYMMDD')}"
            f"&irs2report=eprint&set_name=eprint&set_value={self.eprint_id}&datatype=countries&top=countries"
            "&view=Table&limit=all&export=JSON"
        )
        expected_totals_url = (
            "https://discovery.ucl.ac.uk/cgi/stats/get"
            f"?from={self.start_date.format('YYYYMMDD')}&to={self.end_date.format('YYYYMMDD')}"
            f"&irs2report=eprint&set_name=eprint&set_value={self.eprint_id}&datatype=downloads&graph_type=column"
            "&view=Google%3A%3AGraph&date_resolution=month&title=Download+activity+-+last+12+months&export=JSON"
        )
        http_returns = [
            {"timescale": {"from": self.start_formatted, "to": self.end_formatted}, "set": {"value": self.eprint_id}},
            {"timescale": {"from": self.start_formatted, "to": self.end_formatted}, "set": {"value": self.eprint_id}},
        ]
        mock_retry_get_url.return_value.json.side_effect = http_returns

        # Check that the correct data is returned
        result = download_discovery_stats(self.eprint_id, self.start_date, self.end_date)

        # Check that constructed urls are correct
        expected_calls = [call(expected_countries_url), call().json(), call(expected_totals_url), call().json()]
        mock_retry_get_url.assert_has_calls(expected_calls)

        # Check that returned results are correct
        self.assertEqual(result[0], http_returns[0])
        self.assertEqual(result[1], http_returns[1])

    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.retry_get_url")
    def test_download_discovery_stats_invalid_timescale(self, mock_retry_get_url):
        """Check if exceptions raised when timescale is inconsistent with inputs"""
        mock_retry_get_url.return_value.json.side_effect = [
            {"timescale": {"from": "19700101", "to": "19700130"}, "set": {"value": self.eprint_id}},
            {"timescale": {"from": "19700101", "to": "19700130"}, "set": {"value": self.eprint_id}},
        ]
        self.assertRaisesRegex(
            ValueError, "timescale", download_discovery_stats, self.eprint_id, self.start_date, self.end_date
        )

    @patch("oaebu_workflows.ucl_discovery_telescope.ucl_discovery_telescope.retry_get_url")
    def test_download_discovery_stats_invalid_eprint_id(self, mock_retry_get_url):
        """Check if exceptions raised when eprint ID is inconsistent with inputs"""
        mock_retry_get_url.return_value.json.side_effect = [
            {"timescale": {"from": self.start_formatted, "to": self.end_formatted}, "set": {"value": "67890"}},
            {"timescale": {"from": self.start_formatted, "to": self.end_formatted}, "set": {"value": "67890"}},
        ]
        self.assertRaisesRegex(
            ValueError, "eprint ID", download_discovery_stats, self.eprint_id, self.start_date, self.end_date
        )


class TestTransformDiscoveryStats(TestCase):
    def test_transform_discovery_stats(self):
        """Test the transform_discovery_stats function when inputs are valid"""
        country_record = {
            "set": {"value": "eprint_id_1"},
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "records": [{"value": "AA", "count": "42"}, {"value": "BB", "count": "42"}],
        }
        totals_record = {
            "set": {"value": "eprint_id_1"},
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "records": [{"count": "84", "datestamp": "202306", "description": "foo"}],
        }
        isbn = "ISBN_1"
        title = "title1"
        expected_output = {
            "ISBN": "ISBN_1",
            "title": "title1",
            "eprint_id": "eprint_id_1",
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "total_downloads": "84",
            "country": [{"value": "AA", "count": "42"}, {"value": "BB", "count": "42"}],
        }
        self.assertEqual(transform_discovery_stats(country_record, totals_record, isbn, title), expected_output)

    def test_transform_discovery_stats_no_country_records(self):
        """Test the transform_discovery_stats function when country records are missing"""
        country_record = {
            "set": {"value": "eprint_id_1"},
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            # Missing records
        }
        totals_record = {
            "set": {"value": "eprint_id_1"},
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "records": [{"count": "84", "datestamp": "202306", "description": "foo"}],
        }
        isbn = "ISBN_1"
        title = "title1"
        expected_output = {
            "ISBN": "ISBN_1",
            "title": "title1",
            "eprint_id": "eprint_id_1",
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "total_downloads": "84",
            "country": [],
        }
        self.assertEqual(transform_discovery_stats(country_record, totals_record, isbn, title), expected_output)

    def test_transform_discovery_stats_mismatching_eprint_ids(self):
        """Test the transform_discovery_stats function when eprint IDs do not match"""
        country_record = {
            "set": {"value": "eprint_id_1"},
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "records": [{"value": "AA", "count": "42"}, {"value": "BB", "count": "42"}],
        }
        totals_record = {
            "set": {"value": "eprint_id_2"},
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "records": [{"count": "84", "datestamp": "202306", "description": "foo"}],
        }
        isbn = "ISBN_3"
        title = "title3"
        with self.assertRaisesRegex(ValueError, "eprint ID do not match"):
            transform_discovery_stats(country_record, totals_record, isbn, title)

    def test_transform_discovery_stats_mismatching_timescales(self):
        """Test the transform_discovery_stats function when timescales do not match"""
        country_record = {
            "set": {"value": "eprint_id_1"},
            "timescale": {"to": "19700131", "format": "YYYYMMDD", "from": "19700101"},
            "origin": "foo",
            "records": [{"value": "AA", "count": "42"}, {"value": "BB", "count": "42"}],
        }
        totals_record = {
            "set": {"value": "eprint_id_1"},
            "timescale": {"to": "20230630", "format": "YYYYMMDD", "from": "20230601"},
            "origin": "foo",
            "records": [{"count": "84", "datestamp": "202306", "description": "foo"}],
        }
        isbn = "ISBN_3"
        title = "title3"
        with self.assertRaisesRegex(ValueError, "Timescales do not match"):
            transform_discovery_stats(country_record, totals_record, isbn, title)
