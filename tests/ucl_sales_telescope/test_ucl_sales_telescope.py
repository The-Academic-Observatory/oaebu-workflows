# Copyright 2020-2024 Curtin University
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
from unittest import TestCase
from unittest.mock import patch

import pendulum
from airflow.utils.state import State
from airflow.models.connection import Connection

from oaebu_workflows.config import test_fixtures_folder, module_file_path
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope import (
    UclSalesRelease,
    create_dag,
    download,
    transform,
    data_integrity_check,
)
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.date_utils import datetime_normalise
from observatory_platform.google.bigquery import bq_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase, load_and_parse_json


class TestUclSalesTelescope(SandboxTestCase):
    """Tests for the Ucl Sales telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests."""
        super(TestUclSalesTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        fixtures_folder = test_fixtures_folder(workflow_module="ucl_sales_telescope")
        self.test_table = os.path.join(fixtures_folder, "test_table.json")

    def test_dag_structure(self):
        """Test that the UCL Sales DAG has the correct structure."""

        dag = create_dag(dag_id="Test_Dag", cloud_workspace=self.fake_cloud_workspace, sheet_id="foo")
        self.assert_dag_structure(
            {
                "check_dependencies": ["_make_release"],
                "_make_release": [
                    "_download",
                    "_transform",
                    "_data_integrity_check",
                    "_bq_load",
                    "_add_new_dataset_releases",
                    "_cleanup_workflow",
                ],
                "_download": ["_transform"],
                "_transform": ["_data_integrity_check"],
                "_data_integrity_check": ["_bq_load"],
                "_bq_load": ["_add_new_dataset_releases"],
                "_add_new_dataset_releases": ["_cleanup_workflow"],
                "_cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the UCL Sales DAG can be loaded from a DAG bag."""
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id="ucl_sales",
                    name="UCL Sales Telescope",
                    class_name="oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(sheet_id="foo"),
                )
            ]
        )
        with env.create():
            dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
            self.assert_dag_load_from_config("ucl_sales", dag_file)

    def test_telescope(self):
        """Test the UCL Sales telescope end to end."""
        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Setup DAG
        data_partner = partner_from_str("ucl_sales")
        data_partner.bq_dataset_id = env.add_dataset()
        dag_id = "ucl_sales"
        api_bq_dataset_id = env.add_dataset()
        dag = create_dag(
            dag_id=dag_id,
            cloud_workspace=env.cloud_workspace,
            sheet_id="foo",
            data_partner=data_partner,
            api_bq_dataset_id=api_bq_dataset_id,
        )
        execution_date = pendulum.datetime(year=2024, month=2, day=4)

        # Create the Observatory environment and run tests
        with env.create(), env.create_dag_run(dag, execution_date):
            # Mock return values of download function
            sheet_return = [
                ["Year", "Month", "Free/Paid/Return?", "Country", "ISBN", "Book", "Pub Date", "Qty"],
                ["2024", "2", "Paid", "UK", "9781111111111", "My Book1", "2023/01/01", "5"],
                ["2024", "2", "Return", "UK", "9782222222222", "My Book2", "2023/01/01", "5"],
                ["2024", "2", "Free", "UK", "9783333333333", "My Book3", "2023/01/01", "5"],
                ["2024", "1", "Paid", "UK", "9784444444444", "My Book4", "2023/01/01", "5"],
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
            ti = env.run_task("check_dependencies")
            self.assertEqual(ti.state, State.SUCCESS)

            # Make the release
            ti = env.run_task("_make_release")
            self.assertEqual(ti.state, State.SUCCESS)
            release_dict = ti.xcom_pull(task_ids="_make_release", include_prior_dates=False)
            expected_release_dict = {
                "dag_id": "ucl_sales",
                "run_id": "scheduled__2024-02-04T00:00:00+00:00",
                "data_interval_start": "2024-02-01",
                "data_interval_end": "2024-03-01",
                "partition_date": "2024-02-29",
            }
            self.assertEqual(release_dict, expected_release_dict)
            release = UclSalesRelease.from_dict(release_dict)

            # download
            sa_patch = patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.service_account")
            build_patch = patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.discovery.build")
            with sa_patch, build_patch as mock_build:
                mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
                mock_service.execute.return_value = {"values": sheet_return}
                ti = env.run_task("_download")
            self.assertEqual(ti.state, State.SUCCESS)

            # transform
            ti = env.run_task("_transform")
            self.assertEqual(ti.state, State.SUCCESS)

            # check data integrity
            ti = env.run_task("_data_integrity_check")
            self.assertEqual(ti.state, State.SUCCESS)

            # bq_load
            ti = env.run_task("_bq_load")
            self.assertEqual(ti.state, State.SUCCESS)

            #######################
            ### Make Assertions ###
            #######################

            # Download
            self.assertTrue(os.path.exists(release.download_path))

            # Check downloaded files uploaded
            download_blob = gcs_blob_name_from_path(release.download_path)
            self.assert_blob_integrity(env.download_bucket, download_blob, release.download_path)

            # Transform
            self.assertTrue(os.path.exists(release.transform_path))

            # Check transformed files uploaded
            self.assert_blob_integrity(
                env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
            )

            # Bigquery load
            table_id = bq_table_id(
                env.cloud_workspace.project_id,
                data_partner.bq_dataset_id,
                data_partner.bq_table_name,
            )
            self.assert_table_integrity(table_id, 4)
            self.assert_table_content(
                table_id,
                load_and_parse_json(self.test_table, date_fields={"release_date", "Publication_Date"}),
                "ISBN13",
            )

            ###################
            ### Final tasks ###
            ###################

            # Set up the API
            api = DatasetAPI(bq_project_id=env.cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id)
            api.seed_db()
            dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="ucl_sales")
            self.assertEqual(len(dataset_releases), 0)

            # Add_dataset_release_task
            now = pendulum.now()
            with patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.pendulum.now") as mock_now:
                mock_now.return_value = now
                ti = env.run_task("_add_new_dataset_releases")
            self.assertEqual(ti.state, State.SUCCESS)
            dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="ucl_sales")
            self.assertEqual(len(dataset_releases), 1)
            expected_release = {
                "dag_id": dag_id,
                "entity_id": "ucl_sales",
                "dag_run_id": release.run_id,
                "created": datetime_normalise(now),
                "modified": datetime_normalise(now),
                "data_interval_start": "2024-02-04T00:00:00+00:00",
                "data_interval_end": "2024-03-04T00:00:00+00:00",
                "snapshot_date": None,
                "partition_date": "2024-02-29T00:00:00+00:00",
                "changefile_start_date": None,
                "changefile_end_date": None,
                "sequence_start": None,
                "sequence_end": None,
                "extra": {},
            }
            self.assertEqual(expected_release, dataset_releases[0].to_dict())

            # Test that all telescope data deleted
            workflow_folder_path = release.workflow_folder
            ti = env.run_task("_cleanup_workflow")
            self.assertEqual(ti.state, State.SUCCESS)
            self.assert_cleanup(workflow_folder_path)


class TestDownloadSales(TestCase):
    """Tests for the download_discovery_stats function"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Set the cutoff date for the tests
        self.start_date = pendulum.datetime(2022, 1, 1)
        self.end_date = pendulum.datetime(2022, 1, 31)
        self.start_formatted = self.start_date.format("YYYYMMDD")
        self.end_formatted = self.end_date.format("YYYYMMDD")
        self.eprint_id = "12345"

    @patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.service_account")
    @patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.discovery.build")
    def test_download_sales_stats_valid(self, mock_build, mock_get_connection, mock_sa):
        """Tests that the download function works when everything is valid"""
        # Mock the Google Sheets API response
        sheet_contents = [
            ["value1", "value2", "value3", "value4", "value5"],
            ["value1", "value2", "value3", "value4", "value5"],
            ["value1", "value2", "value3", "value4", "value5"],
        ]
        mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
        mock_service.execute.return_value = {"values": sheet_contents}

        # Call the function to test
        downloaded_contents = download("sheet_id", "service_account_conn_id", "202001")

        # Assert that the returned mappings match the expected mappings
        self.assertEqual(downloaded_contents, sheet_contents)

    @patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.service_account")
    @patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.ucl_sales_telescope.ucl_sales_telescope.discovery.build")
    def test_download_sales_stats_empty(self, mock_build, mock_get_connection, mock_sa):
        """Tests that the download function works when everything is valid"""
        # Mock the Google Sheets API response
        sheet_contents = []
        mock_service = mock_build.return_value.spreadsheets.return_value.values.return_value.get.return_value
        mock_service.execute.return_value = {"values": sheet_contents}

        # Call the function to test
        with self.assertRaisesRegex(ValueError, "No content found for sheet with ID"):
            download("sheet_id", "service_account_conn_id", "202001")


class TestTransform(TestCase):
    def test_valid_input(self):
        """Test the transform function works when the input is valid"""
        input = [
            ["IsBn", "QTY", "year", "month", "free/paid/return?", "country", "book", "pub date", "foo"],
            ["1111111111111", "1", "2024", "5", "Paid", "UK", "My Book Title", "2020/01/01", "bar"],
            ["2222222222222", "1", "2024", "6", "Return", "UK", "My Book Title", "2020/01/01", "bar"],
        ]
        expected_output = [
            {
                "ISBN13": "1111111111111",
                "Quantity": "1",
                "Year": "2024",
                "Month": "5",
                "Sale_Type": "Paid",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": "2020-01-01",
                "release_date": "2024-05-31",
            },
            {
                "ISBN13": "2222222222222",
                "Quantity": "1",
                "Year": "2024",
                "Month": "6",
                "Sale_Type": "Return",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": "2020-01-01",
                "release_date": "2024-06-30",
            },
        ]
        actual_output = transform(input)
        self.assertEqual(expected_output, actual_output)

    def test_invalid_input(self):
        """Test the transform fails when input is not valid"""
        # missing isbn
        input = [
            ["qty", "year", "month", "free/paid/return?", "country", "book", "pub date", "foo"],
            ["1", "2024", "5", "Paid", "UK", "My Book Title", "2020/01/01", "bar"],
        ]
        with self.assertRaisesRegex(ValueError, "Invalid header found"):
            transform(input)

    def test_invalid_publication_date(self):
        """Test the transform fails when input is not valid"""
        # invalid Publication_Date
        input = [
            ["isbn", "qty", "year", "month", "free/paid/return?", "country", "book", "pub date", "foo"],
            ["2222222222222", "1", "2024", "6", "Return", "UK", "My Book Title", "invalid_date", "bar"],
        ]
        expected_output = [
            {
                "ISBN13": "2222222222222",
                "Quantity": "1",
                "Year": "2024",
                "Month": "6",
                "Sale_Type": "Return",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": None,
                "release_date": "2024-06-30",
            },
        ]
        actual_output = transform(input)
        self.assertEqual(expected_output, actual_output)


class TestDataIntegrityCheck(TestCase):
    def test_valid_input(self):
        """Test that the data integrity check passes when the data is valid"""
        input = [
            {
                "ISBN13": "9781111111111",
                "Quantity": "1",
                "Year": "2024",
                "Month": "5",
                "Sale_Type": "Paid",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": "2020/01/01",
                "release_date": "2024-05-31",
            }
        ]
        self.assertTrue(data_integrity_check(input, pendulum.datetime(2024, 6, 1)))

    def test_empty_data(self):
        """Test that the check fails when and empty dataset is input"""
        self.assertFalse(data_integrity_check([], pendulum.datetime(2024, 6, 1)))

    def test_invalid_date(self):
        """Test that the check fails when an invalid date is input"""
        input = [
            {
                "ISBN13": "9781111111111",
                "Quantity": "1",
                "Year": "2024",
                "Month": "5",
                "Sale_Type": "Paid",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": "2020/01/01",
                "release_date": "2024-05-31",
            }
        ]
        self.assertFalse(data_integrity_check(input, pendulum.datetime(2024, 5, 1)))

    def test_invalid_sale_type(self):
        """Test that the check fails when an invalid sale type is input"""
        input = [
            {
                "ISBN13": "9781111111111",
                "Quantity": "1",
                "Year": "2024",
                "Month": "5",
                "Sale_Type": "Foo",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": "2020/01/01",
                "release_date": "2024-05-31",
            }
        ]
        self.assertFalse(data_integrity_check(input, pendulum.datetime(2024, 6, 1)))

    def test_invalid_isbn(self):
        """Test that the check fails when an invalid isbn is input"""
        input = [
            {
                "ISBN13": "978111111111",  # 12 digits
                "Quantity": "1",
                "Year": "2024",
                "Month": "5",
                "Sale_Type": "Paid",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": "2020/01/01",
                "release_date": "2024-05-31",
            }
        ]
        self.assertFalse(data_integrity_check(input, pendulum.datetime(2024, 6, 1)))
        input[0]["ISBN13"] = "1111111111111"  # Doesn't start with 978
        self.assertFalse(data_integrity_check(input, pendulum.datetime(2024, 6, 1)))

    def test_invalid_quantity(self):
        """Test that the check fails when an invalid quantity is input"""
        input = [
            {
                "ISBN13": "9781111111111",
                "Quantity": "-1",
                "Year": "2024",
                "Month": "5",
                "Sale_Type": "Paid",
                "Country": "UK",
                "Title": "My Book Title",
                "Publication_Date": "2020/01/01",
                "release_date": "2024-05-31",
            }
        ]
        self.assertFalse(data_integrity_check(input, pendulum.datetime(2024, 6, 1)))
