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

# Author: Aniek Roelofs

import json
import os
from unittest.mock import ANY, MagicMock, patch
from urllib.parse import quote
from requests import Response

import httplib2
import httpretty
import pendulum
from airflow.models import Connection
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.utils.state import State
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import RequestMockBuilder

from oaebu_workflows.config import test_fixtures_folder, module_file_path
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope import (
    IRUS_FUNCTION_SOURCE_URL,
    IRUS_FUNCTION_BLOB_NAME,
    IRUS_FUNCTION_NAME,
    IRUS_FUNCTION_REGION,
    IrusOapenRelease,
    call_cloud_function,
    cloud_function_exists,
    create_cloud_function,
    upload_source_code_to_bucket,
    create_dag,
)
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.airflow.workflow import CloudWorkspace, Workflow
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_upload_file
from observatory_platform.google.bigquery import bq_table_id
from observatory_platform.sandbox.test_utils import SandboxTestCase, find_free_port, random_id
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment


class TestIrusOapenTelescope(SandboxTestCase):
    """Tests for the Oapen Irus Uk telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestIrusOapenTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.publisher_name_v4 = (quote("UCL Press"),)
        self.publisher_uuid_v5 = ("df73bf94-b818-494c-a8dd-6775b0573bc2",)
        self.download_path = os.path.join(
            test_fixtures_folder(workflow_module="irus_oapen_telescope"), "download.jsonl.gz"
        )

    def test_dag_structure(self):
        """Test that the Oapen Irus Uk DAG has the correct structure."""
        dag = create_dag(
            dag_id="irus_oapen_test_dag",
            cloud_workspace=self.fake_cloud_workspace,
            publisher_name_v4=self.publisher_name_v4,
            publisher_uuid_v5=self.publisher_uuid_v5,
        )
        self.assert_dag_structure(
            {
                "check_dependencies": ["fetch_releases"],
                "fetch_releases": [
                    "process_release.transfer",
                    "process_release.transform",
                    "process_release.call_cloud_function_",
                    "create_cloud_function_",
                    "process_release.bq_load",
                    "process_release.add_new_dataset_releases",
                    "process_release.cleanup_workflow",
                ],
                "create_cloud_function_": ["process_release.call_cloud_function_"],
                "process_release.call_cloud_function_": ["process_release.transfer"],
                "process_release.transfer": ["process_release.transform"],
                "process_release.transform": ["process_release.bq_load"],
                "process_release.bq_load": ["process_release.add_new_dataset_releases"],
                "process_release.add_new_dataset_releases": ["process_release.cleanup_workflow"],
                "process_release.cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Oapen Irus Uk DAG can be loaded from a DAG bag."""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id="irus_oapen_test",
                    name="My Oapen Irus UK Workflow",
                    class_name="oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publisher_name_v4=self.publisher_name_v4, publisher_uuid_v5=self.publisher_uuid_v5),
                )
            ],
        )
        with env.create():
            dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
            self.assert_dag_load_from_config("irus_oapen_test", dag_file)

    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.build")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.ServiceAccountCredentials")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.AuthorizedSession.post")
    def test_telescope(self, mock_authorized_session, mock_account_credentials, mock_build):
        """Test the IRUS OAPEN telescope end to end."""

        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Setup DAG
        execution_date = pendulum.datetime(year=2021, month=2, day=14)
        data_partner = partner_from_str("irus_oapen")
        data_partner.bq_dataset_id = env.add_dataset()
        dag_id = "irus_oapen_test"
        gdpr_bucket_id = env.add_bucket()
        api_dataset_id = env.add_dataset()
        dag = create_dag(
            dag_id=dag_id,
            cloud_workspace=env.cloud_workspace,
            publisher_name_v4=self.publisher_name_v4,
            publisher_uuid_v5=self.publisher_uuid_v5,
            data_partner=data_partner,
            gdpr_oapen_project_id=env.project_id,
            gdpr_oapen_bucket_id=gdpr_bucket_id,
            api_dataset_id=api_dataset_id,
        )
        # Fake oapen project and bucket

        # Mock the Google Cloud Functions API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ""
        request_builder = RequestMockBuilder(
            {
                "cloudfunctions.projects.locations.functions.get": (
                    None,
                    json.dumps(
                        {
                            "name": "projects/project-id/locations/us-central1/functions/function-2",
                            "serviceConfig": {"uri": "https://oapen-access-stats-kkinbzaigla-ew.a.run.app"},
                        }
                    ),
                ),
                "cloudfunctions.projects.locations.functions.patch": (
                    None,
                    json.dumps(
                        {
                            "name": "projects/project-id/locations/us-central1/operations/d29ya2Z",
                            "done": True,
                            "response": {"message": "response"},
                        }
                    ),
                ),
            },
            check_unexpected=True,
        )
        mock_build.return_value = build(
            "cloudfunctions",
            "v2beta",
            cache_discovery=False,
            static_discovery=False,
            requestBuilder=request_builder,
        )

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):

                # Add airflow connections
                geoip_license_conn_id = "geoip_license_key"
                conn = Connection(conn_id=geoip_license_conn_id, uri="http://email_address:password@")
                env.add_connection(conn)
                irus_oapen_api_conn_id = "irus_api"
                conn = Connection(conn_id=irus_oapen_api_conn_id, uri="mysql://requestor_id:api_key@")
                env.add_connection(conn)
                irus_oapen_login_conn_id = "irus_login"
                conn = Connection(conn_id=irus_oapen_login_conn_id, uri="mysql://user_id:license_key@")
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(ti.state, State.SUCCESS)

                # Make the release
                ti = env.run_task("fetch_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                release_dicts = ti.xcom_pull(task_ids="fetch_releases", include_prior_dates=False)
                expected_release_dicts = [
                    {
                        "dag_id": "irus_oapen_test",
                        "run_id": "scheduled__2021-02-14T00:00:00+00:00",
                        "data_interval_start": "2021-02-01T00:00:00+00:00",
                        "data_interval_end": "2021-03-01T00:00:00+00:00",
                        "partition_date": "2021-02-28T23:59:59.999999+00:00",
                    }
                ]
                self.assertEqual(release_dicts, expected_release_dicts)
                release = IrusOapenRelease.from_dict(release_dicts[0])

                # Test create cloud function task: no error should be thrown
                ti = env.run_task("create_cloud_function_")
                self.assertEqual(ti.state, State.SUCCESS)

                # Test call cloud function task: no error should be thrown
                with httpretty.enabled():
                    # mock response of getting publisher uuid
                    url = "https://library.oapen.org/rest/search?query=publisher.name:ucl_press&expand=metadata"
                    httpretty.register_uri(httpretty.GET, url, body='[{"uuid":"df73bf94-b818-494c-a8dd-6775b0573bc2"}]')
                    # mock response of cloud function
                    mock_authorized_session.return_value = MagicMock(
                        spec=Response,
                        status_code=200,
                        json=lambda: {"entries": 100, "unprocessed_publishers": None},
                        reason="unit test",
                    )
                    url = "https://oapen-access-stats-kkinbzaigla-ew.a.run.app"
                    httpretty.register_uri(httpretty.POST, url, body="")
                    ti = env.run_task("process_release.call_cloud_function_", map_index=0)
                    self.assertEqual(ti.state, State.SUCCESS)

                # Test transfer task
                gcs_upload_file(
                    bucket_name=gdpr_bucket_id,
                    blob_name=release.download_blob_name,
                    file_path=self.download_path,
                )
                ti = env.run_task("process_release.transfer", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(env.download_bucket, release.download_blob_name, self.download_path)

                # Test transform task
                ti = env.run_task("process_release.transform", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertTrue(os.path.exists(release.transform_path))
                self.assert_file_integrity(release.transform_path, "0b111b2f", "gzip_crc")
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )

                # Test that data loads into BigQuery
                ti = env.run_task("process_release.bq_load", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_table_id(
                    project_id=env.cloud_workspace.project_id,
                    dataset_id=data_partner.bq_dataset_id,
                    table_id=data_partner.bq_table_name,
                )
                self.assert_table_integrity(table_id, 2)

                # Delete oapen bucket
                env._delete_bucket(gdpr_bucket_id)

                # Add_dataset_release_task
                api = DatasetAPI(project_id=self.project_id, dataset_id=api_dataset_id)
                api.seed_db()
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)

                # Add_dataset_release_task
                now = pendulum.now("UTC")  # Use UTC to ensure +00UTC timezone
                with patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.pendulum.now") as mock_now:
                    mock_now.return_value = now
                    ti = env.run_task("process_release.add_new_dataset_releases", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, dataset_id=api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "dataset_id": api_dataset_id,
                    "dag_run_id": release.run_id,
                    # Replace Z shorthand because BQ converts it to +00:00
                    "created": now.to_iso8601_string().replace("Z", "+00:00"),
                    "modified": now.to_iso8601_string().replace("Z", "+00:00"),
                    "data_interval_start": "2021-02-01T00:00:00+00:00",
                    "data_interval_end": "2021-03-01T00:00:00+00:00",
                    "snapshot_date": None,
                    "partition_date": "2021-02-28T23:59:59.999999+00:00",
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": {},
                }
                self.assertEqual(expected_release, dataset_releases[0].to_dict())

                # Test that all telescope data deleted
                workflow_folder_path = release.workflow_folder
                ti = env.run_task("process_release.cleanup_workflow", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(workflow_folder_path)

    @patch("observatory.platform.airflow.Variable.get")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.upload_source_code_to_bucket")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.cloud_function_exists")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.create_cloud_function")
    def test_create_cloud_function(self, mock_create_function, mock_function_exists, mock_upload, mock_variable_get):
        """Test the create_cloud_function method of the IrusOapenRelease

        :param mock_variable_get: Mock Airflow Variable 'data'
        """

        def reset_mocks():
            mock_upload.reset_mock()
            mock_function_exists.reset_mock()
            mock_create_function.reset_mock()

        def assert_mocks(create: bool, update: bool):
            mock_upload.assert_called_once_with(
                IRUS_FUNCTION_SOURCE_URL,
                gdpr_oapen_project_id,
                gdpr_oapen_bucket_id,
                IRUS_FUNCTION_BLOB_NAME,
                release.cloud_function_path,
            )
            mock_function_exists.assert_called_once_with(ANY, full_name)
            if create or update:
                mock_create_function.assert_called_once_with(
                    ANY,
                    location,
                    full_name,
                    gdpr_oapen_bucket_id,
                    IRUS_FUNCTION_BLOB_NAME,
                    max_active_runs,
                    update,
                )
            else:
                mock_create_function.assert_not_called()

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.path.join(os.getcwd(), "data")
            # Create release instance
            cloud_workspace = CloudWorkspace(
                project_id="test-project",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
                data_location="us",
            )
            dag_id = "irus_oapen_test"
            publisher_name_v4 = "publisher"
            publisher_uuid_v5 = "publisherUUID"
            bq_dataset_id = "dataset_id"
            gdpr_oapen_project_id = "oapen-usage-data-gdpr-proof"
            gdpr_oapen_bucket_id = "oapen-usage-data-gdpr-proof_cloud-function"
            max_active_runs = 5
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=cloud_workspace,
                publisher_name_v4=publisher_name_v4,
                publisher_uuid_v5=publisher_uuid_v5,
                bq_dataset_id=bq_dataset_id,
                gdpr_oapen_project_id=gdpr_oapen_project_id,
                gdpr_oapen_bucket_id=gdpr_oapen_bucket_id,
                max_active_runs=max_active_runs,
            )
            release = IrusOapenRelease(dag_id=dag_id, run_id=random_id(), partition_date=pendulum.parse("2020-02-01"))
            location = f"projects/{gdpr_oapen_project_id}/locations/{IRUS_FUNCTION_REGION}"
            full_name = f"{location}/functions/{IRUS_FUNCTION_NAME}"

            env = SandboxEnvironment(
                self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
            )
            with env.create_dag_run(dag, pendulum.datetime(year=2023, month=1, day=1)):

                ti = env.run_task("fetch_releases")

                # Test when source code upload was unsuccessful
                mock_upload.return_value = False, False
                with self.assertRaises(AirflowException):
                    env.run_task("create_cloud_function")
                dag.clear(task_ids=["create_cloud_function"])

                # Test when cloud function does not exist
                reset_mocks()
                mock_upload.return_value = True, True
                mock_function_exists.return_value = False
                mock_create_function.return_value = True, "response"
                env.run_task("create_cloud_function")
                assert_mocks(create=True, update=False)
                dag.clear(task_ids=["create_cloud_function"])

                # Test when cloud function exists, but source code has changed
                reset_mocks()
                mock_upload.return_value = True, True
                mock_function_exists.return_value = True
                mock_create_function.return_value = True, "response"
                env.run_task("create_cloud_function")
                assert_mocks(create=False, update=True)
                dag.clear(task_ids=["create_cloud_function"])

                # Test when cloud function exists and source code has not changed
                reset_mocks()
                mock_upload.return_value = True, False
                mock_function_exists.return_value = True
                env.run_task("create_cloud_function")
                assert_mocks(create=False, update=False)
                dag.clear(task_ids=["create_cloud_function"])

                # Test when create cloud function was unsuccessful
                reset_mocks()
                mock_upload.return_value = True, True
                mock_function_exists.return_value = True
                mock_create_function.return_value = False, "response"
                with self.assertRaises(AirflowException):
                    env.run_task("create_cloud_function")

    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.gcs_upload_file")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.gcs_create_bucket")
    def test_upload_source_code_to_bucket(self, mock_create_bucket, mock_upload_to_bucket):
        """Test getting source code from oapen irus uk release and uploading to storage bucket.
        Test expected results both when md5 hashes match and when they don't."""
        mock_create_bucket.return_value = True
        mock_upload_to_bucket.return_value = True, True
        with CliRunner().isolated_filesystem():
            cloud_function_path = os.path.join(os.getcwd(), "cloud_function.zip")
            success, upload = upload_source_code_to_bucket(
                source_url=IRUS_FUNCTION_SOURCE_URL,
                project_id="project_id",
                bucket_name="bucket",
                blob_name=IRUS_FUNCTION_BLOB_NAME,
                cloud_function_path=cloud_function_path,
            )
            self.assertEqual(success, True)
            self.assertEqual(upload, True)

            # Check that an error is raised if the md5 hash doesn't match
            with self.assertRaises(AirflowException):
                upload_source_code_to_bucket(
                    source_url=IRUS_FUNCTION_SOURCE_URL,
                    project_id="project_id",
                    bucket_name="bucket",
                    blob_name=IRUS_FUNCTION_BLOB_NAME,
                    cloud_function_path=cloud_function_path,
                    expected_md5_hash="different",
                )

    def test_cloud_function_exists(self):
        """Test the function that checks whether the cloud function exists"""
        requests = [
            # Cloud function exists
            {
                "response": (
                    None,
                    json.dumps(
                        {
                            "name": "projects/project-id/locations/us-central1/functions/function-2",
                            "serviceConfig": {"uri": "https://oapen-access-stats-kkinbzaigla-ew.a.run.app"},
                        }
                    ),
                ),
                "uri": "https://oapen-access-stats-kkinbzaigla-ew.a.run.app",
            },
            # Cloud function does not exist
            {"response": (httplib2.Response({"status": 404, "reason": "HttpError"}), b"{}"), "uri": None},
        ]
        for request in requests:
            with self.subTest(request=request):
                request_builder = RequestMockBuilder(
                    {
                        "cloudfunctions.projects.locations.functions.get": request["response"],
                    },
                    check_unexpected=True,
                )
                service = build(
                    "cloudfunctions",
                    "v2beta",
                    cache_discovery=False,
                    static_discovery=False,
                    requestBuilder=request_builder,
                )

                full_name = "projects/project-id/locations/us-central1/functions/function-2"
                uri = cloud_function_exists(service, full_name=full_name)
                self.assertEqual(request["uri"], uri)

    def test_create_cloud_function(self):
        """Test the function that creates the cloud function"""
        location = "projects/project-id/locations/us-central1"
        full_name = "projects/project-id/locations/us-central1/functions/function-2"
        source_bucket = "oapen_bucket"
        blob_name = "source_code.zip"
        max_active_runs = 1

        requests = [
            # Creating cloud function, no error
            {
                "method_id": "cloudfunctions.projects.locations.functions.create",
                "response": (
                    None,
                    json.dumps(
                        {
                            "name": "projects/project-id/locations/us-central1/operations/d29ya2Z",
                            "done": True,
                            "response": {"message": "response"},
                        }
                    ),
                ),
                "success": True,
                "msg": {"message": "response"},
            },
            # Updating/patching cloud function, no error
            {
                "method_id": "cloudfunctions.projects.locations.functions.patch",
                "response": (
                    None,
                    json.dumps(
                        {
                            "name": "projects/project-id/locations/us-central1/operations/d29ya2Z",
                            "done": True,
                            "response": {"message": "response"},
                        }
                    ),
                ),
                "success": True,
                "msg": {"message": "response"},
            },
            # Creating cloud function, error
            {
                "method_id": "cloudfunctions.projects.locations.functions.create",
                "response": (
                    None,
                    json.dumps(
                        {
                            "name": "projects/project-id/locations/us-central1/operations/d29ya2Z",
                            "done": True,
                            "error": {"message": "error"},
                        }
                    ),
                ),
                "success": False,
                "msg": {"message": "error"},
            },
        ]

        for request in requests:
            with self.subTest(request=request):
                request_builder = RequestMockBuilder(
                    {
                        request["method_id"]: (
                            None,
                            json.dumps(
                                {
                                    "name": "projects/project-id/locations/us-central1/operations/d29ya2Z",
                                    "done": False,
                                }
                            ),
                        ),
                        "cloudfunctions.projects.locations.operations.get": request["response"],
                    },
                    check_unexpected=True,
                )
                service = build(
                    "cloudfunctions",
                    "v2beta",
                    cache_discovery=False,
                    static_discovery=False,
                    requestBuilder=request_builder,
                )

                update = request["method_id"] == "cloudfunctions.projects.locations.functions.patch"
                success, msg = create_cloud_function(
                    service, location, full_name, source_bucket, blob_name, max_active_runs, update=update
                )
                self.assertEqual(request["success"], success)
                self.assertDictEqual(request["msg"], msg)

    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.AuthorizedSession.post")
    def test_call_cloud_function(self, mock_authorized_session):
        """Test the function that calls the cloud function"""

        function_uri = "function_uri"
        release_date = "2020-01-01"
        username = "username"
        password = "password"
        geoip_license_key = "key"
        publisher_name = "publisher_name"
        publisher_uuid = "publisher_uuid"
        bucket_name = "bucket"
        blob_name = "blob"

        # Set responses for consequential calls
        mock_authorized_session.side_effect = [
            MagicMock(
                spec=Response,
                status_code=200,
                reason="unit test",
                json=lambda: {"entries": 100, "unprocessed_publishers": ["publisher1", "publisher2"]},
            ),
            MagicMock(
                spec=Response,
                status_code=200,
                reason="unit test",
                json=lambda: {"entries": 200, "unprocessed_publishers": None},
            ),
            MagicMock(
                spec=Response,
                status_code=200,
                reason="unit test",
                json=lambda: {"entries": 0, "unprocessed_publishers": None},
            ),
            MagicMock(spec=Response, status_code=400, reason="unit test"),
        ]
        # Test when there are unprocessed publishers (first 2 responses from side effect)
        call_cloud_function(
            function_uri=function_uri,
            release_date=release_date,
            username=username,
            password=password,
            geoip_license_key=geoip_license_key,
            publisher_name_v4=publisher_name,
            publisher_uuid_v5=publisher_uuid,
            bucket_name=bucket_name,
            blob_name=blob_name,
        )
        self.assertEqual(2, mock_authorized_session.call_count)

        # Test when entries is 0 (3rd response from side effect)
        with self.assertRaises(AirflowSkipException):
            call_cloud_function(
                function_uri=function_uri,
                release_date=release_date,
                username=username,
                password=password,
                geoip_license_key=geoip_license_key,
                publisher_name_v4=publisher_name,
                publisher_uuid_v5=publisher_uuid,
                bucket_name=bucket_name,
                blob_name=blob_name,
            )

        # Test when response status code is not 200 (last response from side effect)
        with self.assertRaises(AirflowException):
            call_cloud_function(
                function_uri=function_uri,
                release_date=release_date,
                username=username,
                password=password,
                geoip_license_key=geoip_license_key,
                publisher_name_v4=publisher_name,
                publisher_uuid_v5=publisher_uuid,
                bucket_name=bucket_name,
                blob_name=blob_name,
            )
