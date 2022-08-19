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

# Author: Richard Hosking


from cmath import exp
import os
from datetime import timedelta
from unittest.mock import MagicMock, patch
import vcr
import pendulum
from click.testing import CliRunner

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oapen_workflow import OapenWorkflow, OapenWorkflowRelease
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import (
    run_bigquery_query,
)
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    make_dummy_dag,
    find_free_port,
)
from observatory.platform.utils.workflow_utils import (
    make_dag_id,
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


class TestOapenWorkflow(ObservatoryTestCase):
    """
    Test the OapenWorkflow class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.org_name = "OAPEN Press"
        self.gcp_project_id = "project_id"
        self.data_location = os.getenv("TESTS_DATA_LOCATION")

        # Release Object Defaults for reference
        self.ao_gcp_project_id = "academic-observatory"

    @patch("oaebu_workflows.workflows.oapen_workflow.OapenWorkflow.make_release")
    @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
    def test_cleanup(self, mock_sel_table_suffixes, mock_mr):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2022, 1, 1)]
        with CliRunner().isolated_filesystem():
            wf = OapenWorkflow()

            mock_mr.return_value = OapenWorkflowRelease(
                release_date=pendulum.datetime(2022, 1, 1),
                gcp_project_id=self.gcp_project_id,
            )

            release = wf.make_release(execution_date=pendulum.datetime(2022, 1, 1))
            wf.cleanup(release)

    def test_dag_structure(self):

        with CliRunner().isolated_filesystem():
            wf = OapenWorkflow()
            dag = wf.make_dag()
            self.assert_dag_structure(
                {
                    "oapen_irus_uk_oapen_press_sensor": ["check_dependencies"],
                    "oapen_metadata_sensor": ["check_dependencies"],
                    "check_dependencies": ["create_onix_formatted_metadata_output_tasks"],
                    "create_onix_formatted_metadata_output_tasks": [
                        "create_oaebu_crossref_metadata_table",
                        "create_oaebu_crossref_events_table",
                    ],
                    "create_oaebu_crossref_metadata_table": ["create_oaebu_book_table"],
                    "create_oaebu_crossref_events_table": ["create_oaebu_book_table"],
                    "create_oaebu_book_table": ["create_oaebu_book_product_table"],
                    "create_oaebu_book_product_table": ["export_oaebu_table.book_product_list"],
                    "export_oaebu_table.book_product_list": ["export_oaebu_table.book_product_metrics"],
                    "export_oaebu_table.book_product_metrics": ["export_oaebu_table.book_product_metrics_country"],
                    "export_oaebu_table.book_product_metrics_country": ["export_oaebu_table.book_product_metrics_city"],
                    "export_oaebu_table.book_product_metrics_city": ["export_oaebu_table.book_product_metrics_events"],
                    "export_oaebu_table.book_product_metrics_events": [
                        "export_oaebu_table.book_product_publisher_metrics"
                    ],
                    "export_oaebu_table.book_product_publisher_metrics": [
                        "export_oaebu_table.book_product_subject_bic_metrics"
                    ],
                    "export_oaebu_table.book_product_subject_bic_metrics": [
                        "export_oaebu_table.book_product_year_metrics"
                    ],
                    "export_oaebu_table.book_product_year_metrics": [
                        "export_oaebu_table.book_product_subject_year_metrics"
                    ],
                    "export_oaebu_table.book_product_subject_year_metrics": [
                        "export_oaebu_table.book_product_author_metrics"
                    ],
                    "export_oaebu_table.book_product_author_metrics": ["cleanup"],
                    "cleanup": ["add_new_dataset_releases"],
                    "add_new_dataset_releases": [],
                },
                dag,
            )


class TestOapenWorkflowFunctional(ObservatoryTestCase):
    """Functionally test the workflow."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.timestamp = pendulum.now()
        self.oapen_table_id = "oapen"

        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.org_name = OapenWorkflow.ORG_NAME
        self.gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")

        self.gcp_dataset_id = "oaebu"
        self.irus_uk_dag_id_prefix = "oapen_irus_uk"
        self.irus_uk_table_id = "oapen_irus_uk"

        # vcrpy cassettes for http request mocking
        self.metadata_cassette = test_fixtures_folder("oapen_workflow", "crossref_metadata_request.yaml")
        self.events_cassette = test_fixtures_folder("oapen_workflow", "crossref_events_request.yaml")

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Oapen Workflow"
        workflow_type = WorkflowType(name=name, type_id=OapenWorkflow.DAG_ID_PREFIX)
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
            type_id=OapenWorkflow.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Oapen Example Dataset",
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

    def setup_fake_data(self, settings_dataset_id: str, fixtures_dataset_id: str, release_date: pendulum.DateTime):
        # Settings dataset
        country = load_jsonl(test_fixtures_folder("onix_workflow", "country.jsonl"))
        settings_schema_path = test_fixtures_folder("onix_workflow", "schema")
        # Fixtures dataset
        metadata = load_jsonl(test_fixtures_folder("oapen_workflow", "oapen_metadata.jsonl"))
        oapen_irus_uk = load_jsonl(test_fixtures_folder("oapen_workflow", "oapen_irus_uk.jsonl"))
        bic_lookup = load_jsonl(test_fixtures_folder("oapen_workflow", "bic_lookup.jsonl"))
        fixtures_schema_path = test_fixtures_folder("oapen_workflow", "schema")
        tables = [
            Table(
                "country",
                False,
                settings_dataset_id,
                country,
                "country",
                settings_schema_path,
            ),
            Table(
                "oapen_metadata",
                False,
                fixtures_dataset_id,
                metadata,
                "oapen_metadata",
                fixtures_schema_path,
            ),
            Table(
                "oapen_irus_uk",
                False,
                fixtures_dataset_id,
                oapen_irus_uk,
                "oapen_irus_uk",
                fixtures_schema_path,
            ),
            Table(
                "bic_lookup",
                False,
                fixtures_dataset_id,
                bic_lookup,
                "bic_lookup",
                fixtures_schema_path,
            ),
        ]

        bq_load_tables(
            tables=tables,
            bucket_name=self.gcp_bucket_name,
            release_date=release_date,
            data_location=self.data_location,
            project_id=self.gcp_project_id,
        )

    def test_run_workflow_tests(self):
        """Functional test of the OAPEN workflow"""

        def vcr_ignore_condition(request):
            """This function is used by vcrpy to allow requests to sources not in the cassette file.
            At time of writing, the only mocked requests are the ones to crossref events and metadata."""
            allowed_domains = ["https://api.crossref.org", "https://api.eventdata.crossref.org"]
            allow_request = any([request.url.startswith(i) for i in allowed_domains])
            if not allow_request:
                return None
            return request

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location, api_port=self.port, api_host=self.host)
        org_name = self.org_name

        # Create datasets
        oaebu_intermediate_dataset_id = env.add_dataset(prefix="oaebu_intermediate")
        oaebu_output_dataset_id = env.add_dataset(prefix="oaebu")
        oaebu_onix_dataset_id = env.add_dataset(prefix="oaebu_onix_dataset")
        oaebu_elastic_dataset_id = env.add_dataset(prefix="data_export")
        oaebu_settings_dataset_id = env.add_dataset(prefix="settings")
        oaebu_fixtures_dataset_id = env.add_dataset(prefix="fixtures")
        oaebu_crossref_dataset_id = env.add_dataset(prefix="crossref")

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.gcp_bucket_name = env.transform_bucket
            self.setup_connections(env)
            self.setup_api()

            # Setup workflow
            start_date = pendulum.datetime(year=2022, month=1, day=1)
            crossref_start_date = pendulum.datetime(year=2018, month=5, day=14)
            workflow = OapenWorkflow(
                oaebu_onix_dataset=oaebu_onix_dataset_id,
                oaebu_onix_table_id="onix",
                oaebu_dataset=oaebu_output_dataset_id,
                oaebu_intermediate_dataset=oaebu_intermediate_dataset_id,
                oaebu_elastic_dataset=oaebu_elastic_dataset_id,
                irus_uk_dataset_id=oaebu_fixtures_dataset_id,
                ao_gcp_project_id=self.gcp_project_id,
                oapen_gcp_project_id=self.gcp_project_id,
                oapen_metadata_dataset_id=oaebu_fixtures_dataset_id,
                oapen_metadata_table_id="oapen_metadata",
                start_date=start_date,
                crossref_start_date=crossref_start_date,
                country_project_id=self.gcp_project_id,
                country_dataset_id=oaebu_settings_dataset_id,
                subject_project_id=self.gcp_project_id,
                subject_dataset_id=oaebu_fixtures_dataset_id,
                workflow_id=1,
                max_threads=1,  # Having more than 1 thread for testing will give inconsistent returns due to VCR
            )

            # Override sensor grace period and dag check
            for sensor in workflow.operators[0]:
                sensor.grace_period = timedelta(seconds=1)
                sensor.check_exists = False

            # Make DAG
            workflow_dag = workflow.make_dag()

            # If the DAG you are monitoring doesn't exist in dagrun database, it will return success to skip waiting.
            expected_state = "success"
            with env.create_dag_run(workflow_dag, start_date):
                ti = env.run_task(f"{make_dag_id(self.irus_uk_dag_id_prefix, org_name)}_sensor")
                self.assertEqual(expected_state, ti.state)

                ti = env.run_task(f"oapen_metadata_sensor")
                self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            expected_state = "success"
            execution_date = pendulum.datetime(year=2022, month=1, day=8)
            release_date = pendulum.datetime(year=2022, month=1, day=14)

            # Setup fake data
            self.setup_fake_data(oaebu_settings_dataset_id, oaebu_fixtures_dataset_id, release_date)

            dag = make_dummy_dag(make_dag_id(self.irus_uk_dag_id_prefix, org_name), execution_date)
            with env.create_dag_run(dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task")
                self.assertEqual(expected_state, ti.state)

            dag = make_dummy_dag("oapen_metadata", execution_date)
            with env.create_dag_run(dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task")
                self.assertEqual(expected_state, ti.state)

            # Run end to end tests for the DAG
            with env.create_dag_run(workflow_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                ti = env.run_task(f"{make_dag_id(self.irus_uk_dag_id_prefix, org_name)}_sensor")
                self.assertEqual(expected_state, ti.state)

                ti = env.run_task(f"oapen_metadata_sensor")
                self.assertEqual(expected_state, ti.state)

                # Check dependencies
                ti = env.run_task("check_dependencies")
                self.assertEqual(expected_state, ti.state)

                # Mock make_release
                workflow.make_release = MagicMock(
                    return_value=OapenWorkflowRelease(
                        release_date=release_date,
                        gcp_project_id=self.gcp_project_id,
                        gcp_bucket_name=env.transform_bucket,
                        crossref_dataset_id=oaebu_crossref_dataset_id,
                    )
                )

                # Create tables and test conditions
                release_suffix = release_date.strftime("%Y%m%d")

                # Format OAPEN Metadata like ONIX to enable the next steps
                ti = env.run_task(workflow.create_onix_formatted_metadata_output_tasks.__name__)
                self.assertEqual(expected_state, ti.state)
                onix_table_id = f"{self.gcp_project_id}.{oaebu_onix_dataset_id}.onix{release_suffix}"
                self.assert_table_content(
                    onix_table_id, load_jsonl(test_fixtures_folder("oapen_workflow", "expected_onix.jsonl"))
                )

                # Load crossref metadata table into bigquery
                metadata_vcr = vcr.VCR(record_mode="none", before_record_request=vcr_ignore_condition)
                with metadata_vcr.use_cassette(self.metadata_cassette):
                    ti = env.run_task(workflow.create_oaebu_crossref_metadata_table.__name__)
                # Assertions
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{self.gcp_project_id}.{oaebu_crossref_dataset_id}.crossref_metadata{release_suffix}",
                    load_jsonl(test_fixtures_folder("oapen_workflow", "expected_crossref_metadata.jsonl")),
                )

                # Load crossref event table into bigquery
                events_vcr = vcr.VCR(record_mode="none", before_record_request=vcr_ignore_condition)
                with events_vcr.use_cassette(self.events_cassette):
                    ti = env.run_task(workflow.create_oaebu_crossref_events_table.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{self.gcp_project_id}.{oaebu_crossref_dataset_id}.crossref_events{release_suffix}",
                    load_jsonl(test_fixtures_folder("oapen_workflow", "expected_crossref_events.jsonl")),
                )

                # Create book table in bigquery
                ti = env.run_task(workflow.create_oaebu_book_table.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{self.gcp_project_id}.{oaebu_output_dataset_id}.book{release_suffix}",
                    load_jsonl(test_fixtures_folder("oapen_workflow", "expected_book.jsonl")),
                )

                # Create book product table
                ti = env.run_task(workflow.create_oaebu_book_product_table.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{self.gcp_project_id}.{oaebu_output_dataset_id}.book_product{release_suffix}",
                    load_jsonl(test_fixtures_folder("oapen_workflow", "expected_book_product.jsonl")),
                )

                # Export oaebu elastic table names and expected row count
                export_tables = [
                    ("book_product_list", 3),
                    ("book_product_metrics", 2),
                    ("book_product_metrics_country", 2),
                    ("book_product_metrics_city", 2),
                    ("book_product_metrics_events", 0),
                    ("book_product_publisher_metrics", 1),
                    ("book_product_subject_bic_metrics", 1),
                    ("book_product_year_metrics", 1),
                    ("book_product_subject_year_metrics", 1),
                    ("book_product_author_metrics", 2),
                ]

                for table, exp_rows in export_tables:
                    ti = env.run_task(f"{workflow.export_oaebu_table.__name__}.{table}")
                    self.assertEqual(expected_state, ti.state, msg=f"table: {table}")
                    # Check that the data_export tables tables exist and have the correct number of rows
                    table_id = f"{self.gcp_project_id}.{oaebu_elastic_dataset_id}.{self.gcp_project_id.replace('-', '_')}_{table}{release_suffix}"
                    self.assert_table_integrity(table_id, expected_rows=exp_rows)

                # Ensure there are no duplicates
                sql = f"""  SELECT
                                count
                            FROM(SELECT
                                COUNT(*) as count
                                FROM {self.gcp_project_id}.{oaebu_elastic_dataset_id}.{self.gcp_project_id.replace('-', '_')}_book_product_metrics{release_suffix}
                                GROUP BY product_id, month)
                            WHERE count > 1"""
                records = run_bigquery_query(sql)
                self.assertEqual(len(records), 0)

                # Cleanup
                env.run_task(workflow.cleanup.__name__)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)
