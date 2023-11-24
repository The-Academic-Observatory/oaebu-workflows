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

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope import (
    IrusOapenRelease,
    IrusOapenTelescope,
    call_cloud_function,
    cloud_function_exists,
    create_cloud_function,
    upload_source_code_to_bucket,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import CloudWorkspace, Workflow
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_upload_file
from observatory.platform.bigquery import bq_table_id
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    find_free_port,
    random_id,
)


class TestIrusOapenTelescope(ObservatoryTestCase):
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
        dag = IrusOapenTelescope(
            dag_id="irus_oapen_test_dag",
            cloud_workspace=self.fake_cloud_workspace,
            publisher_name_v4=self.publisher_name_v4,
            publisher_uuid_v5=self.publisher_uuid_v5,
        ).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["create_cloud_function"],
                "create_cloud_function": ["call_cloud_function"],
                "call_cloud_function": ["transfer"],
                "transfer": ["download_transform"],
                "download_transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Oapen Irus Uk DAG can be loaded from a DAG bag."""

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="irus_oapen_test",
                    name="My Oapen Irus UK Workflow",
                    class_name="oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.IrusOapenTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(publisher_name_v4=self.publisher_name_v4, publisher_uuid_v5=self.publisher_uuid_v5),
                )
            ],
        )
        with env.create():
            self.assert_dag_load_from_config("irus_oapen_test")

    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.build")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.ServiceAccountCredentials")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.AuthorizedSession.post")
    def test_telescope(self, mock_authorized_session, mock_account_credentials, mock_build):
        """Test the Oapen Irus Uk telescope end to end."""
        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=2, day=14)
        patner = partner_from_str("irus_oapen")
        patner.bq_dataset_id = env.add_dataset()
        telescope = IrusOapenTelescope(
            dag_id="irus_oapen_test",
            cloud_workspace=env.cloud_workspace,
            publisher_name_v4=self.publisher_name_v4,
            publisher_uuid_v5=self.publisher_uuid_v5,
            data_partner=patner,
        )
        # Fake oapen project and bucket
        IrusOapenTelescope.OAPEN_PROJECT_ID = env.project_id
        IrusOapenTelescope.OAPEN_BUCKET = random_id()

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

        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                # Use release to check results from tasks
                # release = IrusOapenRelease(
                #     dag_id=telescope.dag_id, run_id=env.dag_run.run_id, partition_date=execution_date.end_of("month")
                # )
                release = telescope.make_release(
                    run_id=env.dag_run.run_id,
                    data_interval_start=pendulum.parse(str(env.dag_run.data_interval_start)),
                    data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end)),
                )[0]

                # Add airflow connections
                conn = Connection(conn_id=telescope.geoip_license_conn_id, uri="http://email_address:password@")
                env.add_connection(conn)
                conn = Connection(conn_id=telescope.irus_oapen_api_conn_id, uri="mysql://requestor_id:api_key@")
                env.add_connection(conn)
                conn = Connection(conn_id=telescope.irus_oapen_login_conn_id, uri="mysql://user_id:license_key@")
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test create cloud function task: no error should be thrown
                ti = env.run_task(telescope.create_cloud_function.__name__)
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
                    ti = env.run_task(telescope.call_cloud_function.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                # Test transfer task
                gcs_upload_file(
                    bucket_name=IrusOapenTelescope.OAPEN_BUCKET,
                    blob_name=release.blob_name,
                    file_path=self.download_path,
                )
                ti = env.run_task(telescope.transfer.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(env.download_bucket, release.blob_name, self.download_path)

                # Test download_transform task
                ti = env.run_task(telescope.download_transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertTrue(os.path.exists(release.transform_path))
                self.assert_file_integrity(release.transform_path, "0b111b2f", "gzip_crc")

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
                    project_id=telescope.cloud_workspace.project_id,
                    dataset_id=telescope.data_partner.bq_dataset_id,
                    table_id=telescope.data_partner.bq_table_name,
                )
                self.assert_table_integrity(table_id, 2)

                # Delete oapen bucket
                env._delete_bucket(IrusOapenTelescope.OAPEN_BUCKET)

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
                telescope.FUNCTION_SOURCE_URL,
                telescope.OAPEN_PROJECT_ID,
                telescope.OAPEN_BUCKET,
                telescope.FUNCTION_BLOB_NAME,
                release.cloud_function_path,
            )
            mock_function_exists.assert_called_once_with(ANY, full_name)
            if create or update:
                mock_create_function.assert_called_once_with(
                    ANY,
                    location,
                    full_name,
                    telescope.OAPEN_BUCKET,
                    telescope.FUNCTION_BLOB_NAME,
                    telescope.max_active_runs,
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
            telescope = IrusOapenTelescope(
                dag_id="irus_oapen_test",
                cloud_workspace=cloud_workspace,
                publisher_name_v4="publisher",
                publisher_uuid_v5="publisherUUID",
                bq_dataset_id="dataset_id",
            )
            release = IrusOapenRelease(
                dag_id=telescope.dag_id, run_id=random_id(), partition_date=pendulum.parse("2020-02-01")
            )
            location = f"projects/{telescope.OAPEN_PROJECT_ID}/locations/{telescope.FUNCTION_REGION}"
            full_name = f"{location}/functions/{telescope.FUNCTION_NAME}"

            # Test when source code upload was unsuccessful
            mock_upload.return_value = False, False
            task_instance = MagicMock()
            # context = dict(ti=task_instance)
            with self.assertRaises(AirflowException):
                telescope.create_cloud_function(releases=[release], ti=task_instance)

            # Test when cloud function does not exist
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = False
            mock_create_function.return_value = True, "response"
            telescope.create_cloud_function(releases=[release], ti=task_instance)
            assert_mocks(create=True, update=False)

            # Test when cloud function exists, but source code has changed
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = True
            mock_create_function.return_value = True, "response"
            telescope.create_cloud_function(telescope.max_active_runs)
            assert_mocks(create=False, update=True)

            # Test when cloud function exists and source code has not changed
            reset_mocks()
            mock_upload.return_value = True, False
            mock_function_exists.return_value = True
            telescope.create_cloud_function(releases=[release], ti=task_instance)
            assert_mocks(create=False, update=False)

            # Test when create cloud function was unsuccessful
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = True
            mock_create_function.return_value = False, "response"
            with self.assertRaises(AirflowException):
                telescope.create_cloud_function(releases=[release], ti=task_instance)

    @patch("observatory.platform.airflow.Variable.get")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.call_cloud_function")
    @patch("oaebu_workflows.irus_oapen_telescope.irus_oapen_telescope.cloud_function_exists")
    def test_call_cloud_function(self, mock_function_exists, mock_call_function, mock_conn_get, mock_variable_get):
        """Test the call_cloud_function method of the IrusOapenRelease

        :param mock_variable_get: Mock Airflow Variable 'data'
        """
        connections = {
            "geoip_license_key": Connection("geoip_license_key", uri="http://user_id:key@"),
            "irus_oapen_api": Connection("irus_oapen_api", uri="http://requestor_id:api_key@"),
            "irus_oapen_login": Connection("irus_oapen_login", uri="http://email:password@"),
        }
        mock_conn_get.side_effect = lambda x: connections[x]

        # Set URI to function url
        function_url = "https://oapen-access-stats-kkinbzfjal-ew.a.run.app"
        mock_function_exists.return_value = function_url

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.path.join(os.getcwd(), "data")
            cloud_workspace = CloudWorkspace(
                project_id=self.project_id,
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
                data_location="us",
            )

            # Test new platform and old platform
            for date in ["2020-03", "2020-04"]:
                # Test for a given publisher name and the 'oapen' publisher
                for publisher in [("publisher", "uuid1"), ("oapen", "uuid2")]:
                    mock_call_function.reset_mock()

                    telescope = IrusOapenTelescope(
                        dag_id="irus_oapen_test",
                        cloud_workspace=cloud_workspace,
                        publisher_name_v4=publisher[0],
                        publisher_uuid_v5=publisher[1],
                        bq_dataset_id="dataset_id",
                    )
                    release = IrusOapenRelease(
                        dag_id=telescope.dag_id, run_id=random_id(), partition_date=pendulum.parse(date + "-01")
                    )
                    telescope.call_cloud_function(releases=[release])

                    # Test that the call function is called with the correct args
                    if date == "2020-04":
                        username = "requestor_id"
                        password = "api_key"
                    else:
                        username = "email"
                        password = "password"
                    mock_call_function.assert_called_once_with(
                        function_url,
                        date,
                        username,
                        password,
                        "key",
                        telescope.publisher_name_v4,
                        telescope.publisher_uuid_v5,
                        telescope.OAPEN_BUCKET,
                        release.blob_name,
                    )

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
                IrusOapenTelescope.FUNCTION_SOURCE_URL,
                IrusOapenTelescope.OAPEN_PROJECT_ID,
                IrusOapenTelescope.OAPEN_BUCKET,
                IrusOapenTelescope.FUNCTION_BLOB_NAME,
                cloud_function_path,
            )
            self.assertEqual(success, True)
            self.assertEqual(upload, True)

            IrusOapenTelescope.FUNCTION_MD5_HASH = "different"
            with self.assertRaises(AirflowException):
                upload_source_code_to_bucket(
                    IrusOapenTelescope.FUNCTION_SOURCE_URL,
                    IrusOapenTelescope.OAPEN_PROJECT_ID,
                    IrusOapenTelescope.OAPEN_BUCKET,
                    IrusOapenTelescope.FUNCTION_BLOB_NAME,
                    cloud_function_path,
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

        function_url = "function_url"
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
            function_url,
            release_date,
            username,
            password,
            geoip_license_key,
            publisher_name,
            publisher_uuid,
            bucket_name,
            blob_name,
        )
        self.assertEqual(2, mock_authorized_session.call_count)

        # Test when entries is 0 (3rd response from side effect)
        with self.assertRaises(AirflowSkipException):
            call_cloud_function(
                function_url,
                release_date,
                username,
                password,
                geoip_license_key,
                publisher_name,
                publisher_uuid,
                bucket_name,
                blob_name,
            )

        # Test when response status code is not 200 (last response from side effect)
        with self.assertRaises(AirflowException):
            call_cloud_function(
                function_url,
                release_date,
                username,
                password,
                geoip_license_key,
                publisher_name,
                publisher_uuid,
                bucket_name,
                blob_name,
            )
