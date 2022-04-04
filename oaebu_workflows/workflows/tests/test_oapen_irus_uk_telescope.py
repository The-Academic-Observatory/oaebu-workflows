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

import json
import os
from unittest.mock import ANY, MagicMock, patch
from urllib.parse import quote

import httplib2
import httpretty
import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.connection import Connection
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import RequestMockBuilder
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.identifiers import WorkflowTypes
from oaebu_workflows.workflows.oapen_irus_uk_telescope import (
    OapenIrusUkRelease,
    OapenIrusUkTelescope,
    call_cloud_function,
    cloud_function_exists,
    create_cloud_function,
    upload_source_code_to_bucket,
)
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.gc_utils import upload_file_to_cloud_storage
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    random_id,
)
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path
from requests import Response
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.telescope import Telescope
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection
from airflow.utils.state import State

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.identifiers import TelescopeTypes
from oaebu_workflows.workflows.oapen_irus_uk_telescope import (
    OapenIrusUkRelease,
    OapenIrusUkTelescope,
    call_cloud_function,
    cloud_function_exists,
    create_cloud_function,
    upload_source_code_to_bucket,
)


class TestOapenIrusUkTelescope(ObservatoryTestCase):
    """Tests for the Oapen Irus Uk telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestOapenIrusUkTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.organisation_name = "ucl_press"
        self.extra = {
            "publisher_name_v4": quote("UCL Press"),
            "publisher_uuid_v5": "df73bf94-b818-494c-a8dd-6775b0573bc2",
        }
        self.host = "localhost"
        self.api_port = 5000
        self.download_path = test_fixtures_folder("oapen_irus_uk", "download.jsonl.gz")
        self.transform_hash = "0b111b2f"

        # API environment
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "UCL Press"

    def setup_api(self, env=None):
        dt = pendulum.now("UTC")

        name = "Oapen Metadata Telescope"
        workflow_type = WorkflowType(name=name, type_id=OapenIrusUkTelescope.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        gcp_download_bucket = env.download_bucket if env else "download_bucket"
        gcp_transform_bucket = env.transform_bucket if env else "transform_bucket"

        organisation = Organisation(
            name=self.org_name,
            project_id=self.project_id,
            download_bucket=gcp_download_bucket,
            transform_bucket=gcp_transform_bucket,
        )
        self.api.put_organisation(organisation)

        telescope = Telescope(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_telescope(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=OapenIrusUkTelescope.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Oapen Metadata Dataset",
            address="project.dataset.table",
            service="bigquery",
            connection=Telescope(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the Oapen Irus Uk DAG has the correct structure.
        :return: None
        """
        organisation = Organisation(name=self.organisation_name)
        dag = OapenIrusUkTelescope(
            organisation, self.extra["publisher_name_v4"], self.extra["publisher_uuid_v5"]
        ).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["create_cloud_function"],
                "create_cloud_function": ["call_cloud_function"],
                "call_cloud_function": ["transfer"],
                "transfer": ["download_transform"],
                "download_transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Oapen Irus Uk DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "oapen_irus_uk_telescope.py")
            self.assert_dag_load("oapen_irus_uk_ucl_press", dag_file)

    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.build")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.ServiceAccountCredentials")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.AuthorizedSession.post")
    def test_telescope(self, mock_authorized_session, mock_account_credentials, mock_build):
        """Test the Oapen Irus Uk telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=2, day=14)
        organisation = Organisation(
            name=self.organisation_name,
            project_id=self.project_id,
            download_bucket=env.download_bucket,
            transform_bucket=env.transform_bucket,
        )
        telescope = OapenIrusUkTelescope(
            organisation=organisation,
            publisher_name_v4=self.extra.get("publisher_name_v4"),
            publisher_uuid_v5=self.extra.get("publisher_uuid_v5"),
            dataset_id=dataset_id,
            workflow_id=1,
        )
        # Fake oapen project and bucket
        OapenIrusUkTelescope.OAPEN_PROJECT_ID = env.project_id
        OapenIrusUkTelescope.OAPEN_BUCKET = random_id()

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

        # Use release to check results from tasks
        release = OapenIrusUkRelease(telescope.dag_id, execution_date.end_of("month"), organisation)

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.setup_connections(env)
            self.setup_api(env=env)
            with env.create_dag_run(dag, execution_date):
                # Add airflow connections
                conn = Connection(conn_id=AirflowConns.GEOIP_LICENSE_KEY, uri="http://email_address:password@")
                env.add_connection(conn)
                conn = Connection(conn_id=AirflowConns.OAPEN_IRUS_UK_API, uri="mysql://requestor_id:api_key@")
                env.add_connection(conn)
                conn = Connection(conn_id=AirflowConns.OAPEN_IRUS_UK_LOGIN, uri="mysql://user_id:license_key@")
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test create cloud function task: no error should be thrown
                env.run_task(telescope.create_cloud_function.__name__)

                # Test call cloud function task: no error should be thrown
                with httpretty.enabled():
                    # mock response of getting publisher uuid
                    url = (
                        f"https://library.oapen.org/rest/search?query=publisher.name:{release.organisation_id}"
                        f"&expand=metadata"
                    )
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
                    env.run_task(telescope.call_cloud_function.__name__)

                # Test transfer task
                upload_file_to_cloud_storage(OapenIrusUkTelescope.OAPEN_BUCKET, release.blob_name, self.download_path)
                env.run_task(telescope.transfer.__name__)
                self.assert_blob_integrity(env.download_bucket, release.blob_name, self.download_path)

                # Test download_transform task
                env.run_task(telescope.download_transform.__name__)
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
                    expected_rows = 2
                    self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

                # Delete oapen bucket
                env._delete_bucket(OapenIrusUkTelescope.OAPEN_BUCKET)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.upload_source_code_to_bucket")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.cloud_function_exists")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.create_cloud_function")
    def test_release_create_cloud_function(
        self, mock_create_function, mock_function_exists, mock_upload, mock_variable_get
    ):
        """Test the create_cloud_function method of the OapenIrusUkRelease

        :param mock_variable_get: Mock Airflow Variable 'data'
        :return: None.
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
            org = Organisation(
                name=self.organisation_name,
                project_id=self.project_id,
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
            )
            telescope = OapenIrusUkTelescope(
                org, publisher_name_v4="publisher", publisher_uuid_v5="publisherUUID", dataset_id="dataset_id"
            )
            release = OapenIrusUkRelease(telescope.dag_id, pendulum.parse("2020-02-01"), org)

            location = f"projects/{telescope.OAPEN_PROJECT_ID}/locations/{telescope.FUNCTION_REGION}"
            full_name = f"{location}/functions/{telescope.FUNCTION_NAME}"

            # Test when source code upload was unsuccessful
            mock_upload.return_value = False, False
            with self.assertRaises(AirflowException):
                release.create_cloud_function(telescope.max_active_runs)

            # Test when cloud function does not exist
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = False
            mock_create_function.return_value = True, "response"
            release.create_cloud_function(telescope.max_active_runs)
            assert_mocks(create=True, update=False)

            # Test when cloud function exists, but source code has changed
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = True
            mock_create_function.return_value = True, "response"
            release.create_cloud_function(telescope.max_active_runs)
            assert_mocks(create=False, update=True)

            # Test when cloud function exists and source code has not changed
            reset_mocks()
            mock_upload.return_value = True, False
            mock_function_exists.return_value = True
            release.create_cloud_function(telescope.max_active_runs)
            assert_mocks(create=False, update=False)

            # Test when create cloud function was unsuccessful
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = True
            mock_create_function.return_value = False, "response"
            with self.assertRaises(AirflowException):
                release.create_cloud_function(telescope.max_active_runs)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.BaseHook.get_connection")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.call_cloud_function")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.cloud_function_exists")
    def test_release_call_cloud_function(
        self, mock_function_exists, mock_call_function, mock_conn_get, mock_variable_get
    ):
        """Test the call_cloud_function method of the OapenIrusUkRelease

        :param mock_variable_get: Mock Airflow Variable 'data'
        :return: None.
        """
        connections = {
            AirflowConns.GEOIP_LICENSE_KEY: Connection(AirflowConns.GEOIP_LICENSE_KEY, uri="http://user_id:key@"),
            AirflowConns.OAPEN_IRUS_UK_API: Connection(
                AirflowConns.OAPEN_IRUS_UK_API, uri="http://requestor_id:api_key@"
            ),
            AirflowConns.OAPEN_IRUS_UK_LOGIN: Connection(
                AirflowConns.OAPEN_IRUS_UK_LOGIN, uri="http://email:password@"
            ),
        }
        mock_conn_get.side_effect = lambda x: connections[x]

        # Set URI to function url
        function_url = "https://oapen-access-stats-kkinbzfjal-ew.a.run.app"
        mock_function_exists.return_value = function_url

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.path.join(os.getcwd(), "data")
            org = Organisation(
                name=self.organisation_name,
                project_id=self.project_id,
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
            )

            # Test new platform and old platform
            for date in ["2020-03", "2020-04"]:
                # Test for a given publisher name and the 'oapen' publisher
                for publisher in [("publisher", "uuid1"), ("oapen", "uuid2")]:
                    mock_call_function.reset_mock()

                    telescope = OapenIrusUkTelescope(
                        org, publisher_name_v4=publisher[0], publisher_uuid_v5=publisher[1], dataset_id="dataset_id"
                    )
                    release = OapenIrusUkRelease(telescope.dag_id, pendulum.parse(date + "-01"), org)
                    release.call_cloud_function(telescope.publisher_name_v4, telescope.publisher_uuid_v5)

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

    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.upload_file_to_cloud_storage")
    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.create_cloud_storage_bucket")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_upload_source_code_to_bucket(self, mock_variable_get, mock_create_bucket, mock_upload_to_bucket):
        """Test getting source code from oapen irus uk release and uploading to storage bucket.
        Test expected results both when md5 hashes match and when they don't.

        :return: None.
        """
        mock_create_bucket.return_value = True
        mock_upload_to_bucket.return_value = True, True
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.getcwd()
            success, upload = upload_source_code_to_bucket(
                OapenIrusUkTelescope.FUNCTION_SOURCE_URL,
                OapenIrusUkTelescope.OAPEN_PROJECT_ID,
                OapenIrusUkTelescope.OAPEN_BUCKET,
                OapenIrusUkTelescope.FUNCTION_BLOB_NAME,
            )
            self.assertEqual(success, True)
            self.assertEqual(upload, True)

            OapenIrusUkTelescope.FUNCTION_MD5_HASH = "different"
            with self.assertRaises(AirflowException):
                upload_source_code_to_bucket(
                    OapenIrusUkTelescope.FUNCTION_SOURCE_URL,
                    OapenIrusUkTelescope.OAPEN_PROJECT_ID,
                    OapenIrusUkTelescope.OAPEN_BUCKET,
                    OapenIrusUkTelescope.FUNCTION_BLOB_NAME,
                )

    def test_cloud_function_exists(self):
        """Test the function that checks whether the cloud function exists
        :return: None.
        """
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
        """Test the function that creates the cloud function
        :return: None.
        """
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

    @patch("oaebu_workflows.workflows.oapen_irus_uk_telescope.AuthorizedSession.post")
    def test_call_cloud_function(self, mock_authorized_session):
        """Test the function that calls the cloud function
        :return: None.
        """

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
