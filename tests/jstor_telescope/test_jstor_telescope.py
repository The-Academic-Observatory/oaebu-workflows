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

# Author: Aniek Roelofs, Keegan Smith

import base64
import json
import os
from unittest.mock import Mock, patch

import httpretty
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from oaebu_workflows.config import test_fixtures_folder, module_file_path
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.jstor_telescope.jstor_telescope import (
    JSTOR_PROCESSED_LABEL_NAME,
    JstorRelease,
    JstorPublishersAPI,
    JstorCollectionsAPI,
    create_dag,
    make_jstor_api,
)
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.bigquery import bq_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_upload_files
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase, load_and_parse_json


def dummy_gmail_connection() -> Connection:
    return Connection(
        conn_id="gmail_api",
        uri="google-cloud-platform://?token=123&refresh_token=123"
        "&client_id=123.apps.googleusercontent.com&client_secret=123",
    )


class TestTelescopeSetup(SandboxTestCase):
    def __init__(self, *args, **kwargs):
        super(TestTelescopeSetup, self).__init__(*args, **kwargs)
        self.entity_id = "anupress"

    def test_dag_structure(self):
        """Test that the Jstor DAG has the correct structure."""
        env = SandboxEnvironment()
        with env.create():
            env.add_connection(dummy_gmail_connection())
            for entity_type in ["publisher", "collection"]:
                dag = create_dag(
                    dag_id="jstor_test",
                    cloud_workspace=self.fake_cloud_workspace,
                    entity_id=self.entity_id,
                    entity_type=entity_type,
                )
                self.assert_dag_structure(
                    {
                        "check_dependencies": ["list_reports"],
                        "list_reports": ["download"],
                        "download": [
                            "process_release.transform",
                            "process_release.bq_load",
                            "process_release.add_new_dataset_releases",
                            "process_release.cleanup_workflow",
                        ],
                        "process_release.transform": ["process_release.bq_load"],
                        "process_release.bq_load": ["process_release.add_new_dataset_releases"],
                        "process_release.add_new_dataset_releases": ["process_release.cleanup_workflow"],
                        "process_release.cleanup_workflow": [],
                    },
                    dag,
                )

    def test_dag_load(self):
        """Test that the Jstor DAG can be loaded from a DAG bag."""
        for entity_type in ["publisher", "collection"]:
            env = SandboxEnvironment(
                workflows=[
                    Workflow(
                        dag_id="jstor_test_telescope",
                        name="My JSTOR Workflow",
                        class_name="oaebu_workflows.jstor_telescope.jstor_telescope",
                        cloud_workspace=self.fake_cloud_workspace,
                        kwargs=dict(entity_id=self.entity_id, entity_type=entity_type),
                    )
                ],
            )

            with env.create():
                env.add_connection(dummy_gmail_connection())
                dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
                self.assert_dag_load_from_config("jstor_test_telescope", dag_file)

    def test_entity_type(self):
        """Test that the correct entity type is enforced."""
        env = SandboxEnvironment()
        with env.create():
            env.add_connection(dummy_gmail_connection())
            for entity_type in ["collection", "publisher"]:
                make_jstor_api(entity_type, self.entity_id)
            with self.assertRaisesRegex(AirflowException, "Entity type must be"):
                make_jstor_api("invalid", self.entity_id)


class TestJstorTelescopePublisher(SandboxTestCase):
    """Tests for the Jstor telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestJstorTelescopePublisher, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.entity_id = "anupress"

        self.release_date = pendulum.parse("20220701").end_of("month")
        fixtures_folder = test_fixtures_folder(workflow_module="jstor_telescope")
        self.country_report = {
            "path": os.path.join(fixtures_folder, "country_20220801.tsv"),
            "url": "https://www.jstor.org/admin/reports/download/249192019",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_{self.entity_id}_PUBBCU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
            "download_hash": "9330cc71f8228838ac84abb33cedb3b8",
            "transform_hash": "5a72fe64",
            "table_rows": 10,
        }
        self.institution_report = {
            "path": os.path.join(fixtures_folder, "institution_20220801.tsv"),
            "url": "https://www.jstor.org/admin/reports/download/129518301",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_{self.entity_id}_PUBBIU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
            "download_hash": "1c78c316766a3f7306d6c19440250484",
            "transform_hash": "f339ea81",
            "table_rows": 3,
        }
        self.wrong_publisher_report = {
            "path": os.path.join(fixtures_folder, "institution_20220801.tsv"),  # has to be valid path, but is not used
            "url": "https://www.jstor.org/admin/reports/download/12345",
            "headers": {
                "Content-Disposition": f"attachment; filename=PUB_publisher_PUBBIU_"
                f'{self.release_date.strftime("%Y%m%d")}.tsv'
            },
        }

    @patch("oaebu_workflows.jstor_telescope.jstor_telescope.build")
    @patch("oaebu_workflows.jstor_telescope.jstor_telescope.Credentials")
    def test_telescope_publisher(self, mock_account_credentials, mock_build):
        """Test the Jstor telescope end to end."""

        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(
            publisher_http_mock_sequence(
                self.country_report["url"], self.institution_report["url"], self.wrong_publisher_report["url"]
            )
        )
        mock_build.return_value = build("gmail", "v1", http=http)

        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            # Add gmail connection
            env.add_connection(dummy_gmail_connection())

            # Setup Telescope
            logical_date = pendulum.datetime(year=2020, month=11, day=1)
            country_partner = partner_from_str("jstor_country")
            dataset_id = env.add_dataset()
            country_partner.bq_dataset_id = dataset_id
            institution_partner = partner_from_str("jstor_institution")
            institution_partner.bq_dataset_id = dataset_id
            api_bq_dataset_id = env.add_dataset()
            dag_id = "jstor_test_telescope"
            entity_type = "publisher"
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=env.cloud_workspace,
                entity_id=self.entity_id,
                entity_type=entity_type,
                country_partner=country_partner,
                institution_partner=institution_partner,
                api_bq_dataset_id=api_bq_dataset_id,
            )

            # Begin DAG run
            with env.create_dag_run(dag, logical_date=logical_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(ti.state, State.SUCCESS)

                # Test list releases task with files available
                with httpretty.enabled():
                    for report in [self.country_report, self.institution_report, self.wrong_publisher_report]:
                        self.setup_mock_file_download(
                            report["url"], report["path"], headers=report["headers"], method=httpretty.HEAD
                        )
                    ti = env.run_task("list_reports")
                    self.assertEqual(ti.state, State.SUCCESS)
                available_reports = ti.xcom_pull(task_ids="list_reports", include_prior_dates=False)
                self.assertIsInstance(available_reports, list)
                expected_reports_info = [
                    {"type": "country", "url": self.country_report["url"], "id": "1788ec9e91f3de62"},
                    {"type": "institution", "url": self.institution_report["url"], "id": "1788ebe4ecbab055"},
                ]
                self.assertListEqual(expected_reports_info, available_reports)

                # Test download_reports task
                with httpretty.enabled(), patch(
                    "oaebu_workflows.jstor_telescope.jstor_telescope.gcs_upload_files"
                ) as mock_upload:
                    mock_upload.return_value = True
                    for report in [self.country_report, self.institution_report]:
                        self.setup_mock_file_download(report["url"], report["path"], headers=report["headers"])
                    ti = env.run_task("download")
                    self.assertEqual(ti.state, State.SUCCESS)

                # use release info for other tasks
                release_dicts = ti.xcom_pull(task_ids="download", include_prior_dates=False)
                expected_releases = [
                    {
                        "dag_id": "jstor_test_telescope",
                        "run_id": "scheduled__2020-11-01T00:00:00+00:00",
                        "data_interval_start": "2022-07-01",
                        "data_interval_end": "2022-08-01",
                        "partition_date": "2022-07-31",
                        "reports": [
                            {
                                "type": "country",
                                "url": "https://www.jstor.org/admin/reports/download/249192019",
                                "id": "1788ec9e91f3de62",
                            },
                            {
                                "type": "institution",
                                "url": "https://www.jstor.org/admin/reports/download/129518301",
                                "id": "1788ebe4ecbab055",
                            },
                        ],
                    }
                ]
                self.assertEqual(release_dicts, expected_releases)
                release = JstorRelease.from_dict(release_dicts[0])

                # Check that the files were "downloaded"
                self.assertTrue(os.path.exists(release.download_country_path))
                self.assertTrue(os.path.exists(release.download_institution_path))
                self.assert_file_integrity(release.download_country_path, self.country_report["download_hash"], "md5")
                self.assert_file_integrity(
                    release.download_institution_path, self.institution_report["download_hash"], "md5"
                )

                # Do the upload that we patched above
                success = gcs_upload_files(
                    bucket_name=env.cloud_workspace.download_bucket,
                    file_paths=[release.download_institution_path, release.download_country_path],
                )
                self.assertTrue(success)

                # Test that file uploaded
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_country_path),
                    release.download_country_path,
                )
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_institution_path),
                    release.download_institution_path,
                )

                # Test that file transformed
                ti = env.run_task("process_release.transform", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertTrue(os.path.exists(release.transform_country_path))
                self.assertTrue(os.path.exists(release.transform_institution_path))
                self.assert_file_integrity(
                    release.transform_country_path, self.country_report["transform_hash"], "gzip_crc"
                )
                self.assert_file_integrity(
                    release.transform_institution_path, self.institution_report["transform_hash"], "gzip_crc"
                )

                # Test that transformed file was uploaded
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_country_path),
                    release.transform_country_path,
                )
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_institution_path),
                    release.transform_institution_path,
                )

                # Test that data is loaded into BigQuery
                ti = env.run_task("process_release.bq_load", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                country_table_id = bq_table_id(
                    env.cloud_workspace.project_id,
                    country_partner.bq_dataset_id,
                    country_partner.bq_table_name,
                )
                institution_table_id = bq_table_id(
                    env.cloud_workspace.project_id,
                    institution_partner.bq_dataset_id,
                    institution_partner.bq_table_name,
                )
                self.assert_table_integrity(country_table_id, self.country_report["table_rows"])
                self.assert_table_integrity(institution_table_id, self.institution_report["table_rows"])

                # Set up the API
                api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=api_bq_dataset_id)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_country")
                self.assertEqual(len(dataset_releases), 0)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_institution")
                self.assertEqual(len(dataset_releases), 0)

                # Add_dataset_release_task
                now = pendulum.now("UTC")
                with patch("oaebu_workflows.jstor_telescope.jstor_telescope.pendulum.now") as mock_now:
                    mock_now.return_value = now
                    ti = env.run_task("process_release.add_new_dataset_releases", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_country")
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "entity_id": "jstor_country",
                    "dag_run_id": release.run_id,
                    "created": now.to_iso8601_string(),
                    "modified": now.to_iso8601_string(),
                    "data_interval_start": "2022-07-01T00:00:00Z",
                    "data_interval_end": "2022-08-01T00:00:00Z",
                    "snapshot_date": None,
                    "partition_date": "2022-07-31T00:00:00Z",
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": {},
                }
                self.assertEqual(dataset_releases[0].to_dict(), expected_release)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_institution")
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "entity_id": "jstor_institution",
                    "dag_run_id": release.run_id,
                    "created": now.to_iso8601_string(),
                    "modified": now.to_iso8601_string(),
                    "data_interval_start": "2022-07-01T00:00:00Z",
                    "data_interval_end": "2022-08-01T00:00:00Z",
                    "snapshot_date": None,
                    "partition_date": "2022-07-31T00:00:00Z",
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": {},
                }
                self.assertEqual(dataset_releases[0].to_dict(), expected_release)

                # Test that all telescope data deleted
                workflow_folder_path = release.workflow_folder
                ti = env.run_task("process_release.cleanup_workflow", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(workflow_folder_path)

    def test_get_release_date(self):
        """Test that the get_release_date returns the correct release date and raises an exception when dates are
        incorrect"""
        with CliRunner().isolated_filesystem():
            # Test reports in new format with header
            new_report_content = (
                "Report_Name\tBook Usage by Country\nReport_ID\tPUB_BCU\nReport_Description\t"
                "Usage of your books on JSTOR by country.\nPublisher_Name\tPublisher 1\nReporting_Period\t"
                "{start_date} to {end_date}\nCreated\t2021-10-01\nCreated_By\tJSTOR"
            )
            old_report_content = (
                "Book Title\tUsage Month\nAddress source war after\t{start_date}\nNote spend " "government\t{end_date}"
            )
            reports = [
                {"file": "new_success.tsv", "start": "2020-01-01", "end": "2020-01-31"},
                {"file": "new_fail.tsv", "start": "2020-01-01", "end": "2020-02-01"},
                {"file": "old_success.tsv", "start": "2020-01", "end": "2020-01"},
                {"file": "old_fail.tsv", "start": "2020-01", "end": "2020-02"},
            ]

            for report in reports:
                with open(report["file"], "w") as f:
                    if report == reports[0] or report == reports[1]:
                        f.write(new_report_content.format(start_date=report["start"], end_date=report["end"]))
                    else:
                        f.write(old_report_content.format(start_date=report["start"], end_date=report["end"]))

            # Test new report is successful
            api = JstorPublishersAPI(Mock(), self.entity_id)
            start_date, end_date = api.get_release_date(reports[0]["file"])
            self.assertEqual(pendulum.parse(reports[0]["start"]), start_date)
            self.assertEqual(pendulum.parse(reports[0]["end"]), end_date)

            # Test new report fails
            with self.assertRaises(AirflowException):
                api.get_release_date(reports[1]["file"])

            # Test old report is successful
            start_date, end_date = api.get_release_date(reports[2]["file"])
            self.assertEqual(pendulum.parse(reports[2]["start"]).start_of("month").start_of("day"), start_date)
            self.assertEqual(pendulum.parse(reports[2]["end"]).end_of("month").start_of("day"), end_date)

            # Test old report fails
            with self.assertRaises(AirflowException):
                api.get_release_date(reports[3]["file"])


class TestJstorTelescopeCollection(SandboxTestCase):
    """Tests for the Jstor telescope"""

    def __init__(self, *args, **kwargs):
        super(TestJstorTelescopeCollection, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.entity_id = "BTAA"
        self.release_date = pendulum.parse("20230901").end_of("month")

        fixtures_folder = test_fixtures_folder(workflow_module="jstor_telescope")
        self.country_report = {
            "path": os.path.join(fixtures_folder, "collection_country.json"),
            "table": os.path.join(fixtures_folder, "collection_country_table.json"),
            "url": "https://www.jstor.org/admin/reports/download/249192019",
            "download_hash": "8b03d0353ac258bfca48c9347dab76af",
            "transform_hash": "885bf71d",
            "table_rows": 3,
        }
        self.institution_report = {
            "path": os.path.join(fixtures_folder, "collection_institution.json"),
            "table": os.path.join(fixtures_folder, "collection_institution_table.json"),
            "url": "https://www.jstor.org/admin/reports/download/129518301",
            "download_hash": "d52d458ca3a3f0a34f6df4d71688b006",
            "transform_hash": "6ff261d4",
            "table_rows": 6,
        }

    @patch("oaebu_workflows.jstor_telescope.jstor_telescope.build")
    @patch("oaebu_workflows.jstor_telescope.jstor_telescope.Credentials")
    def test_telescope_collection(self, mock_account_credentials, mock_build):
        """Test the Jstor telescope end to end."""

        mock_account_credentials.from_json_keyfile_dict.return_value = ""
        http = HttpMockSequence(
            collection_http_mock_sequence(self.country_report["path"], self.institution_report["path"])
        )
        mock_build.return_value = build("gmail", "v1", http=http)

        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            # Add gmail connection
            env.add_connection(dummy_gmail_connection())

            # Setup DAG
            logical_date = pendulum.datetime(year=2023, month=10, day=4)
            country_partner = partner_from_str("jstor_country_collection")
            dataset_id = env.add_dataset()
            country_partner.bq_dataset_id = dataset_id
            institution_partner = partner_from_str("jstor_institution_collection")
            institution_partner.bq_dataset_id = dataset_id
            api_bq_dataset_id = env.add_dataset()
            dag_id = "jstor_test_telescope"
            entity_type = "collection"
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=env.cloud_workspace,
                entity_id=self.entity_id,
                entity_type=entity_type,
                country_partner=country_partner,
                institution_partner=institution_partner,
                api_bq_dataset_id=api_bq_dataset_id,
            )

            # Begin DAG run
            with env.create_dag_run(dag, logical_date=logical_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(ti.state, State.SUCCESS)

                # Test list releases task with files available
                ti = env.run_task("list_reports")
                self.assertEqual(ti.state, State.SUCCESS)

                available_reports = ti.xcom_pull(task_ids="list_reports", include_prior_dates=False)
                self.assertIsInstance(available_reports, list)
                expected_reports_info = [
                    {"type": "country", "attachment_id": "2", "id": "18af0b40b64fe408"},
                    {"type": "institution", "attachment_id": "3", "id": "18af0b40b64fe408"},
                ]
                self.assertListEqual(expected_reports_info, available_reports)

                # Test download_reports task
                ti = env.run_task("download")
                self.assertEqual(ti.state, State.SUCCESS)

                # use release info for other tasks
                release_dicts = ti.xcom_pull(task_ids="download", include_prior_dates=False)
                expected_releases = [
                    {
                        "dag_id": "jstor_test_telescope",
                        "run_id": "scheduled__2023-10-04T00:00:00+00:00",
                        "data_interval_start": "2023-09-01",
                        "data_interval_end": "2023-10-01",
                        "partition_date": "2023-09-30",
                        "reports": [
                            {"type": "country", "attachment_id": "2", "id": "18af0b40b64fe408"},
                            {"type": "institution", "attachment_id": "3", "id": "18af0b40b64fe408"},
                        ],
                    }
                ]
                self.assertEqual(release_dicts, expected_releases)
                release = JstorRelease.from_dict(release_dicts[0])

                # Check that the files were "downloaded"
                self.assertTrue(os.path.exists(release.download_country_path))
                self.assertTrue(os.path.exists(release.download_institution_path))
                self.assert_file_integrity(release.download_country_path, self.country_report["download_hash"], "md5")
                self.assert_file_integrity(
                    release.download_institution_path, self.institution_report["download_hash"], "md5"
                )

                # Test that file uploaded
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_country_path),
                    release.download_country_path,
                )
                self.assert_blob_integrity(
                    env.download_bucket,
                    gcs_blob_name_from_path(release.download_institution_path),
                    release.download_institution_path,
                )

                # Test that file transformed
                ti = env.run_task("process_release.transform", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertTrue(os.path.exists(release.transform_country_path))
                self.assertTrue(os.path.exists(release.transform_institution_path))
                self.assert_file_integrity(
                    release.transform_country_path, self.country_report["transform_hash"], "gzip_crc"
                )
                self.assert_file_integrity(
                    release.transform_institution_path, self.institution_report["transform_hash"], "gzip_crc"
                )

                # Test that transformed file uploaded
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_country_path),
                    release.transform_country_path,
                )
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.transform_institution_path),
                    release.transform_institution_path,
                )

                # Test that data loaded into BigQuery
                ti = env.run_task("process_release.bq_load", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                country_table_id = bq_table_id(
                    env.cloud_workspace.project_id,
                    country_partner.bq_dataset_id,
                    country_partner.bq_table_name,
                )
                institution_table_id = bq_table_id(
                    env.cloud_workspace.project_id,
                    institution_partner.bq_dataset_id,
                    institution_partner.bq_table_name,
                )
                self.assert_table_integrity(country_table_id, self.country_report["table_rows"])
                self.assert_table_integrity(institution_table_id, self.institution_report["table_rows"])
                expected = load_and_parse_json(self.country_report["table"], date_fields=["release_date"])
                self.assert_table_content(country_table_id, expected, primary_key="ISBN")
                expected = load_and_parse_json(self.institution_report["table"], date_fields=["release_date"])
                self.assert_table_content(institution_table_id, expected, primary_key="ISBN")

                # Set up the API
                api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=api_bq_dataset_id)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_country")
                self.assertEqual(len(dataset_releases), 0)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_institution")
                self.assertEqual(len(dataset_releases), 0)

                # Add_dataset_release_task
                now = pendulum.now("UTC")
                with patch("oaebu_workflows.jstor_telescope.jstor_telescope.pendulum.now") as mock_now:
                    mock_now.return_value = now
                    ti = env.run_task("process_release.add_new_dataset_releases", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_country")
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "entity_id": "jstor_country",
                    "dag_run_id": release.run_id,
                    "created": now.to_iso8601_string(),
                    "modified": now.to_iso8601_string(),
                    "data_interval_start": "2023-09-01T00:00:00Z",
                    "data_interval_end": "2023-10-01T00:00:00Z",
                    "snapshot_date": None,
                    "partition_date": "2023-09-30T00:00:00Z",
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": {},
                }
                self.assertEqual(dataset_releases[0].to_dict(), expected_release)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="jstor_institution")
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "entity_id": "jstor_institution",
                    "dag_run_id": release.run_id,
                    "created": now.to_iso8601_string(),
                    "modified": now.to_iso8601_string(),
                    "data_interval_start": "2023-09-01T00:00:00Z",
                    "data_interval_end": "2023-10-01T00:00:00Z",
                    "snapshot_date": None,
                    "partition_date": "2023-09-30T00:00:00Z",
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": {},
                }
                self.assertEqual(dataset_releases[0].to_dict(), expected_release)

                # Test that all telescope data deleted
                workflow_folder_path = release.workflow_folder
                ti = env.run_task("process_release.cleanup_workflow", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(workflow_folder_path)

    def test_get_release_date(self):
        """Test that the get_release_date returns the correct release date and raises an exception when dates are
        incorrect"""
        with CliRunner().isolated_filesystem():
            # Test reports in new format with header
            new_report_content = (
                'entity_name,book_title,item_doi,publisher,"Month, Year of monthdt"\n'
                "Entity,Book 1,10/1001,Publisher 1,{month_1}\n"  # Start date eg. September 2023
                "Entity,Book 2,10/1002,Publisher 1,{month_2}\n"
            )
            reports = [
                {"file": "new_success.tsv", "report_month_1": "January 2020", "report_month_2": "January 2020"},
                {"file": "new_fail.tsv", "report_month_1": "January 2020", "report_month_2": "February 2020"},
            ]

            for report in reports:
                with open(report["file"], "w") as f:
                    f.write(
                        new_report_content.format(month_1=report["report_month_1"], month_2=report["report_month_2"])
                    )

            # Test new report is successful
            api = JstorCollectionsAPI(Mock(), self.entity_id)
            start_date, end_date = api.get_release_date(reports[0]["file"])
            self.assertEqual(pendulum.parse("2020-01-01"), start_date)
            self.assertEqual(pendulum.parse("2020-01-31"), end_date)
            # Test new report fails
            with self.assertRaises(AirflowException):
                api.get_release_date(reports[1]["file"])


@patch("oaebu_workflows.jstor_telescope.jstor_telescope.build")
@patch("oaebu_workflows.jstor_telescope.jstor_telescope.Credentials")
def test_get_label_id(self, mock_account_credentials, mock_build):
    """Test getting label id both when label already exists and does not exist yet."""
    mock_account_credentials.from_json_keyfile_dict.return_value = ""

    http = HttpMockSequence(
        publisher_http_mock_sequence(
            self.country_report["url"], self.institution_report["url"], self.wrong_publisher_report["url"]
        )
    )
    mock_build.return_value = build("gmail", "v1", http=http)

    list_labels_no_match = {
        "labels": [
            {
                "id": "CHAT",
                "name": "CHAT",
                "messageListVisibility": "hide",
                "labelListVisibility": "labelHide",
                "type": "system",
            },
            {"id": "SENT", "name": "SENT", "type": "system"},
        ]
    }
    create_label = {
        "id": "created_label",
        "name": JSTOR_PROCESSED_LABEL_NAME,
        "messageListVisibility": "show",
        "labelListVisibility": "labelShow",
    }
    list_labels_match = {
        "labels": [
            {
                "id": "CHAT",
                "name": "CHAT",
                "messageListVisibility": "hide",
                "labelListVisibility": "labelHide",
                "type": "system",
            },
            {
                "id": "existing_label",
                "name": JSTOR_PROCESSED_LABEL_NAME,
                "messageListVisibility": "show",
                "labelListVisibility": "labelShow",
                "type": "user",
            },
        ]
    }
    http = HttpMockSequence(
        [
            ({"status": "200"}, json.dumps(list_labels_no_match)),
            ({"status": "200"}, json.dumps(create_label)),
            ({"status": "200"}, json.dumps(list_labels_match)),
        ]
    )
    service = build("gmail", "v1", http=http)
    api = JstorPublishersAPI(service, self.entity_id)

    # call function without match for label, so label is created
    label_id = api.get_label_id(service, JSTOR_PROCESSED_LABEL_NAME)
    self.assertEqual("created_label", label_id)

    # call function with match for label
    label_id = api.get_label_id(service, JSTOR_PROCESSED_LABEL_NAME)
    self.assertEqual("existing_label", label_id)


def publisher_http_mock_sequence(
    country_report_url: str, institution_report_url: str, wrong_publisher_report_url: str
) -> list:
    """Create a list with mocked http responses

    :param country_report_url: URL to country report
    :param institution_report_url: URL to institution report
    :param wrong_publisher_report_url: URL to report with a non-matching publisher id
    :return: List with http responses
    """
    list_labels = {
        "labels": [
            {
                "id": "CHAT",
                "name": "CHAT",
                "messageListVisibility": "hide",
                "labelListVisibility": "labelHide",
                "type": "system",
            },
            {
                "id": "Label_1",
                "name": JSTOR_PROCESSED_LABEL_NAME,
                "messageListVisibility": "show",
                "labelListVisibility": "labelShow",
                "type": "user",
            },
        ]
    }
    list_messages1 = {
        "messages": [
            {"id": "1788ec9e91f3de62", "threadId": "1788e9b0a848236a"},
        ],
        "resultSizeEstimate": 2,
        "nextPageToken": 1234,
    }
    list_messages2 = {
        "messages": [
            {"id": "1788ebe4ecbab055", "threadId": "1788e9b0a848236a"},
            {"id": "5621ayw3vjtag411", "threadId": "1788e9b0a848236a"},
        ],
        "resultSizeEstimate": 2,
    }
    get_message1 = {
        "id": "1788ec9e91f3de62",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service "
        "Account, Your usage report &quot;Book Usage by Country&quot; is now available to "
        "download. Download Completed Report",
        "payload": {
            "partId": "",
            "mimeType": "text/html",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {
                "size": 12313,
                "data": base64.urlsafe_b64encode(
                    f'<a href="{country_report_url}">Download Completed Report</a>'.encode()
                ).decode(),
            },
        },
        "sizeEstimate": 17939,
        "historyId": "2302",
        "internalDate": "1617303299000",
    }
    get_message2 = {
        "id": "1788ebe4ecbab055",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service "
        "Account, Your usage report &quot;Book Usage by Country&quot; is now available to "
        "download. Download Completed Report",
        "payload": {
            "partId": "",
            "mimeType": "text/html",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {
                "size": 12313,
                "data": base64.urlsafe_b64encode(
                    f'<a href="{institution_report_url}">Download Completed Report</a>'.encode()
                ).decode(),
            },
        },
        "sizeEstimate": 17939,
        "historyId": "2302",
        "internalDate": "1617303299000",
    }
    get_message3 = {
        "id": "5621ayw3vjtag411",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service "
        "Account, Your usage report &quot;Book Usage by Country&quot; is now available to "
        "download. Download Completed Report",
        "payload": {
            "partId": "",
            "mimeType": "text/html",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {
                "size": 12313,
                "data": base64.urlsafe_b64encode(
                    f'<a href="{wrong_publisher_report_url}">Download Completed Report</a>'.encode()
                ).decode(),
            },
        },
        "sizeEstimate": 17939,
        "historyId": "2302",
        "internalDate": "1617303299000",
    }
    modify_message1 = {
        "id": "1788ec9e91f3de62",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["Label_1", "CATEGORY_PERSONAL", "INBOX"],
    }
    modify_message2 = {
        "id": "1788ebe4ecbab055",
        "threadId": "1788e9b0a848236a",
        "labelIds": ["Label_1", "CATEGORY_PERSONAL", "INBOX"],
    }
    http_mock_sequence = [
        ({"status": "200"}, json.dumps(list_messages1)),
        ({"status": "200"}, json.dumps(list_messages2)),
        ({"status": "200"}, json.dumps(get_message1)),
        ({"status": "200"}, json.dumps(get_message2)),
        ({"status": "200"}, json.dumps(get_message3)),
        ({"status": "200"}, json.dumps(list_labels)),
        ({"status": "200"}, json.dumps(modify_message1)),
        ({"status": "200"}, json.dumps(modify_message2)),
    ]

    return http_mock_sequence


def collection_http_mock_sequence(country_json: str, institution_json: str) -> list:
    """Create a list with mocked http responses for the collection workflow of the telescope


    :param country_json: The path to the JSON file containing the data for the country message.
    :param institution_json: The path to the JSON file containing the data for the institution message.
    :returns: A list of tuples representing the HTTP mock responses
    """
    list_messages = {
        "messages": [
            {"id": "18af0b40b64fe408", "threadId": "18af0b3f613c3f95"},
            {"id": "18af0b3f613c3f95", "threadId": "18af0b3f613c3f95"},
        ],
        "resultSizeEstimate": 2,
    }

    get_message1 = {
        "id": "18af0b40b64fe408",
        "threadId": "18af0b3f613c3f95",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "Monthly Usage of books by Country and Institution made available through the Big Ten Academic Alliance (BTAA) on JSTOR General Report Notes: Views and downloads of front matter, back matter, and table",
        "payload": {
            "partId": "",
            "mimeType": "multipart/mixed",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {"size": 0},
            "parts": [
                {"partId": "0", "filename": "", "body": {"attachmentId": "0"}},
                {"partId": "1", "filename": "BTAA_Overall_Open_Usage.xlsx", "body": {"attachmentId": "1"}},
                {"partId": "2", "filename": "BTAA_Country_Open_Usage.csv", "body": {"attachmentId": "2"}},
                {"partId": "3", "filename": "BTAA_Open_Institution_Usage.csv", "body": {"attachmentId": "3"}},
            ],
        },
        "sizeEstimate": 355380,
        "historyId": "49808",
        "internalDate": "1696255445000",
    }
    get_message2 = {
        "id": "18af0b3f613c3f95",
        "threadId": "18af0b3f613c3f95",
        "labelIds": ["CATEGORY_PERSONAL", "INBOX"],
        "snippet": "Monthly Usage of books by Country and Institution made available through the Big Ten Academic Alliance (BTAA) on JSTOR General Report Notes: Views and downloads of front matter, back matter, and table",
        "payload": {
            "partId": "",
            "mimeType": "multipart/mixed",
            "filename": "",
            "headers": [{"name": "Delivered-To", "value": "accountname@gmail.com"}],
            "body": {"size": 0},
            # no "parts", this message should be ignored by the telescope
        },
        "sizeEstimate": 355380,
        "historyId": "49808",
        "internalDate": "1696255445000",
    }
    with open(country_json, "rb") as f:
        data_return1 = json.load(f)
        data_return1["data"] = base64.urlsafe_b64encode(data_return1["data"].encode()).decode()
    with open(institution_json, "rb") as f:
        data_return2 = json.load(f)
        data_return2["data"] = base64.urlsafe_b64encode(data_return2["data"].encode()).decode()

    list_labels = {
        "labels": [
            {
                "id": "CHAT",
                "name": "CHAT",
                "messageListVisibility": "hide",
                "labelListVisibility": "labelHide",
                "type": "system",
            },
            {
                "id": "Label_1",
                "name": JSTOR_PROCESSED_LABEL_NAME,
                "messageListVisibility": "show",
                "labelListVisibility": "labelShow",
                "type": "user",
            },
        ]
    }

    modify_message1 = {
        "id": "18af0b40b64fe408",
        "threadId": "18af0b3f613c3f95",
        "labelIds": ["Label_1", "CATEGORY_PERSONAL", "INBOX"],
    }

    http_mock_sequence = [
        ({"status": "200"}, json.dumps(list_messages)),
        ({"status": "200"}, json.dumps(get_message1)),
        ({"status": "200"}, json.dumps(get_message2)),
        ({"status": "200"}, json.dumps(data_return1)),
        ({"status": "200"}, json.dumps(data_return2)),
        ({"status": "200"}, json.dumps(list_labels)),
        ({"status": "200"}, json.dumps(modify_message1)),
    ]
    return http_mock_sequence
