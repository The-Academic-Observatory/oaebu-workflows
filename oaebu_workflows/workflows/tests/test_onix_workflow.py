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

# Author: Tuan Chien

import hashlib
import os
import unittest
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oaebu_partners import OaebuPartner
from oaebu_workflows.workflows.onix_workflow import OnixWorkflow, OnixWorkflowRelease
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from oaebu_workflows.seed.dataset_type_info import get_dataset_type_info
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import (
    run_bigquery_query,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    make_dummy_dag,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.test_utils import (
    random_id,
)
from observatory.platform.utils.workflow_utils import (
    bq_load_partition,
    bq_load_shard_v2,
    make_dag_id,
    table_ids_from_path,
)
from observatory.api.utils import seed_table_type, seed_dataset_type
from oaebu_workflows.seed.table_type_info import get_table_type_info
from oaebu_workflows.seed.dataset_type_info import get_dataset_type_info
from sqlalchemy import table


class TestOnixWorkflowRelease(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        with patch("observatory.platform.utils.gc_utils.select_table_shard_dates") as mock_sel_table_suffixes:
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            self.release = OnixWorkflowRelease(
                dag_id="did",
                release_date=pendulum.datetime(2021, 4, 20),
                onix_release_date=pendulum.datetime(2021, 1, 1),
                gcp_project_id="pid",
                gcp_bucket_name="bucket",
            )

    def test_transform_bucket(self):
        self.assertEqual(self.release.transform_bucket, "bucket")

    def test_transform_folder(self):
        self.assertEqual(self.release.transform_folder, "did/20210420")

    def test_transform_files(self):
        self.assertEqual(
            self.release.transform_files,
            [
                self.release.workslookup_filename,
                self.release.workslookup_errors_filename,
                self.release.worksfamilylookup_filename,
            ],
        )

    def test_download_bucket(self):
        self.assertEqual(self.release.download_bucket, str())

    def test_download_files(self):
        self.assertEqual(self.release.download_files, list())

    def test_extract_files(self):
        self.assertEqual(self.release.extract_files, list())

    def test_download_folder(self):
        self.assertEqual(self.release.download_folder, str())

    def test_extract_folder(self):
        self.assertEqual(self.release.extract_folder, str())


class TestOnixWorkflow(ObservatoryTestCase):
    """
    Test the OnixWorkflow class.
    """

    onix_data = [
        {
            "ISBN13": "111",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "112"},
                    ],
                },
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "113"},
                    ],
                },
            ],
            "RelatedProducts": [
                {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "211"},
            ],
        },
        {
            "ISBN13": "112",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "112"},
                    ],
                },
            ],
            "RelatedProducts": [],
        },
        {
            "ISBN13": "211",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "211"},
                    ],
                },
            ],
            "RelatedProducts": [],
        },
    ]

    class MockTelescopeResponse:
        def __init__(self):
            self.organisation = Organisation(
                name="test",
                project_id="project_id",
            )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.telescope = TestOnixWorkflow.MockTelescopeResponse()
        self.project_id = os.getenv("TESTS_GOOGLE_CLOUD_PROJECT_ID")
        self.data_location = os.getenv("TESTS_DATA_LOCATION")
        self.bucket_name = "bucket_name"

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    def seed_db(self):
        table_type_info = get_table_type_info()
        seed_table_type(table_type_info=table_type_info, api=self.api)

        dataset_type_info = get_dataset_type_info(self.api)
        seed_dataset_type(api=self.api, dataset_type_info=dataset_type_info)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor_gen_dag_id(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="",
                        onix_table_id="onix",
                    )
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))
                    self.assertEqual(wf.dag_id, "onix_workflow_test")
                    self.assertEqual(
                        release.workslookup_filename, "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz"
                    )
                    self.assertEqual(
                        release.workslookup_errors_filename,
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                    )
                    self.assertEqual(
                        release.worksfamilylookup_filename,
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    )

                    self.assertEqual(release.onix_table_id, "onix")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 1, 0, 0, 0, 0))
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.transform_bucket, "bucket_name")
                    self.assertIsInstance(wf.operators[0][0], BaseSensorOperator)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor_gen_assign_dag_id(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        dag_id="dagid",
                    )

                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="dagid",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="",
                        onix_table_id="onix",
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))

                    self.assertEqual(release.dag_id, "dagid")
                    self.assertEqual(release.workslookup_filename, "dagid/20210101/onix_workid_isbn.jsonl.gz")
                    self.assertEqual(
                        release.workslookup_errors_filename, "dagid/20210101/onix_workid_isbn_errors.jsonl.gz"
                    )
                    self.assertEqual(
                        release.worksfamilylookup_filename, "dagid/20210101/onix_workfamilyid_isbn.jsonl.gz"
                    )

                    self.assertEqual(release.onix_table_id, "onix")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 1, 0, 0, 0, 0))
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.transform_bucket, "bucket_name")
                    self.assertIsInstance(wf.operators[0][0], BaseSensorOperator)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))

                    self.assertEqual(wf.dag_id, "onix_workflow_test")
                    self.assertEqual(wf.org_name, "test")
                    self.assertEqual(wf.gcp_bucket_name, "bucket_name")

                    self.assertEqual(release.dag_id, "onix_workflow_test")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 1))
                    self.assertEqual(release.transform_folder, "onix_workflow_test/20210101")
                    self.assertEqual(release.worksid_table, "onix_workid_isbn")
                    self.assertEqual(release.worksid_error_table, "onix_workid_isbn_errors")
                    self.assertEqual(release.workfamilyid_table, "onix_workfamilyid_isbn")

                    self.assertEqual(
                        release.workslookup_filename, "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz"
                    )
                    self.assertEqual(
                        release.workslookup_errors_filename,
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                    )
                    self.assertEqual(
                        release.worksfamilylookup_filename,
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    )

                    self.assertEqual(release.workflow_dataset, "onix_workflow")
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.onix_dataset_id, "onix")
                    self.assertEqual(release.dataset_location, "us")
                    self.assertEqual(release.dataset_description, "ONIX workflow tables")

                    self.assertEqual(release.onix_table_id, "onix")

        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor_dataset_overrides(self, mock_sel_table_suffixes, mock_mr):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            with CliRunner().isolated_filesystem():
                release = OnixWorkflowRelease(
                    dag_id="dag",
                    release_date=pendulum.datetime(2000, 1, 1),
                    onix_release_date=pendulum.datetime(2000, 1, 1),
                    gcp_project_id="project",
                    gcp_bucket_name="bucket",
                )
                self.assertEqual(release.workflow_dataset, "onix_workflow")
                self.assertEqual(release.oaebu_intermediate_dataset, "oaebu_intermediate")
                self.assertEqual(release.oaebu_data_qa_dataset, "oaebu_data_qa")

                # Override
                release = OnixWorkflowRelease(
                    dag_id="dag",
                    release_date=pendulum.datetime(2000, 1, 1),
                    onix_release_date=pendulum.datetime(2000, 1, 1),
                    gcp_project_id="project",
                    gcp_bucket_name="bucket",
                    workflow_dataset="override",
                    oaebu_intermediate_dataset="override",
                    oaebu_data_qa_dataset="override",
                )

                self.assertEqual(release.workflow_dataset, "override")
                self.assertEqual(release.oaebu_intermediate_dataset, "override")
                self.assertEqual(release.oaebu_data_qa_dataset, "override")

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.run_bigquery_query")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_get_onix_records(self, mock_sel_table_suffixes, mock_bq_query, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_bq_query.return_value = TestOnixWorkflow.onix_data
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )
                    records = wf.get_onix_records("project_id", "ds_id", "table_id")
                    self.assertEqual(len(records), 3)
                    self.assertEqual(records[0]["ISBN13"], "111")

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.select_table_shard_dates")
        def test_make_release(self, mock_sel_table_suffixes, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    kwargs = {"next_execution_date": pendulum.datetime(2021, 2, 1)}
                    release = wf.make_release(**kwargs)
                    self.assertIsInstance(release, OnixWorkflowRelease)
                    self.assertEqual(release.dag_id, "onix_workflow_test")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 31))
                    self.assertEqual(release.onix_release_date, pendulum.datetime(2021, 1, 1))
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.onix_dataset_id, "onix")
                    self.assertEqual(release.onix_table_id, "onix")
                    self.assertEqual(release.bucket_name, "bucket_name")

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.select_table_shard_dates")
        def test_make_release_no_releases(self, mock_sel_table_suffixes, mock_api):
            mock_sel_table_suffixes.return_value = []
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    with self.assertRaises(AirflowException):
                        kwargs = {"next_execution_date": pendulum.datetime(2021, 2, 1)}
                        wf.make_release(**kwargs)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_cleanup(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))
                    self.assertTrue(os.path.isdir(release.transform_folder))
                    wf.cleanup(release)
                    self.assertFalse(os.path.isdir(release.transform_folder))

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    def test_dag_structure_ignore_google_analytics(self, mock_api):
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="jstor_country",
                    isbn_field_name="isbn",
                    title_field_name="Book_Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="oapen_irus_uk",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="oapen_irus_uk",
                    isbn_field_name="ISBN",
                    title_field_name="book_title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_sales"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_sales",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_traffic"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_traffic",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
            ]

            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                dag = wf.make_dag()
                self.assert_dag_structure(
                    {
                        "google_books_test_sensor": ["aggregate_works"],
                        "jstor_test_sensor": ["aggregate_works"],
                        "oapen_irus_uk_test_sensor": ["aggregate_works"],
                        "onix_test_sensor": ["aggregate_works"],
                        "aggregate_works": ["upload_aggregation_tables"],
                        "upload_aggregation_tables": ["bq_load_workid_lookup"],
                        "bq_load_workid_lookup": ["bq_load_workid_lookup_errors"],
                        "bq_load_workid_lookup_errors": ["bq_load_workfamilyid_lookup"],
                        "bq_load_workfamilyid_lookup": [
                            "create_oaebu_intermediate_table.dataset.oapen_irus_uk",
                            "create_oaebu_intermediate_table.dataset.google_books_sales",
                            "create_oaebu_intermediate_table.dataset.google_books_traffic",
                            "create_oaebu_intermediate_table.dataset.jstor_country",
                        ],
                        "create_oaebu_intermediate_table.dataset.jstor_country": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.oapen_irus_uk": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.google_books_sales": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_intermediate_table.dataset.google_books_traffic": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_book_product_table": [
                            "export_oaebu_table.book_product_year_metrics",
                            "export_oaebu_table.book_product_subject_bisac_metrics",
                            "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales",
                            "export_oaebu_table.book_product_subject_year_metrics",
                            "export_oaebu_table.book_product_author_metrics",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic",
                            "export_oaebu_table.book_product_metrics_city",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk",
                            "create_oaebu_data_qa_google_books_traffic_isbn",
                            "export_oaebu_table.book_product_metrics_country",
                            "export_oaebu_table.book_product_publisher_metrics",
                            "export_oaebu_table.book_product_subject_bic_metrics",
                            "export_oaebu_table.institution_list",
                            "create_oaebu_data_qa_oapen_irus_uk_isbn",
                            "export_oaebu_table.book_product_list",
                            "export_oaebu_table.book_product_subject_thema_metrics",
                            "create_oaebu_data_qa_google_books_sales_isbn",
                            "create_oaebu_data_qa_onix_aggregate",
                            "export_oaebu_table.book_product_metrics_events",
                            "export_oaebu_table.book_product_metrics",
                            "create_oaebu_data_qa_onix_isbn",
                            "export_oaebu_table.book_product_metrics_institution",
                        ],
                        "create_oaebu_data_qa_onix_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_onix_aggregate": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_oapen_irus_uk_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_sales_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_traffic_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic": [
                            "export_oaebu_qa_metrics"
                        ],
                        "export_oaebu_table.book_product_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_country": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_institution": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.institution_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_city": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_events": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_publisher_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bic_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bisac_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_thema_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_author_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_qa_metrics": ["cleanup"],
                        "cleanup": ["add_new_dataset_releases"],
                        "add_new_dataset_releases": [],
                    },
                    dag,
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    def test_dag_structure_with_google_analytics(self, mock_api):
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="jstor_country",
                    isbn_field_name="isbn",
                    title_field_name="Book_Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="oapen_irus_uk",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="oapen_irus_uk",
                    isbn_field_name="ISBN",
                    title_field_name="book_title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_sales"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_sales",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_traffic"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_traffic",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_analytics"].type_id,
                    dag_id_prefix="google_analytics",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_analytics",
                    isbn_field_name="publication_id",
                    title_field_name="title",
                    sharded=False,
                ),
            ]

            org_name = "ANU Press"
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=org_name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                dag = wf.make_dag()
                self.assert_dag_structure(
                    {
                        "google_analytics_anu_press_sensor": ["aggregate_works"],
                        "google_books_anu_press_sensor": ["aggregate_works"],
                        "jstor_anu_press_sensor": ["aggregate_works"],
                        "oapen_irus_uk_anu_press_sensor": ["aggregate_works"],
                        "onix_anu_press_sensor": ["aggregate_works"],
                        "aggregate_works": ["upload_aggregation_tables"],
                        "upload_aggregation_tables": ["bq_load_workid_lookup"],
                        "bq_load_workid_lookup": ["bq_load_workid_lookup_errors"],
                        "bq_load_workid_lookup_errors": ["bq_load_workfamilyid_lookup"],
                        "bq_load_workfamilyid_lookup": [
                            "create_oaebu_intermediate_table.dataset.google_books_sales",
                            "create_oaebu_intermediate_table.dataset.oapen_irus_uk",
                            "create_oaebu_intermediate_table.dataset.google_analytics",
                            "create_oaebu_intermediate_table.dataset.google_books_traffic",
                            "create_oaebu_intermediate_table.dataset.jstor_country",
                        ],
                        "create_oaebu_intermediate_table.dataset.jstor_country": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.oapen_irus_uk": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.google_books_sales": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_intermediate_table.dataset.google_books_traffic": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_intermediate_table.dataset.google_analytics": ["create_oaebu_book_product_table"],
                        "create_oaebu_book_product_table": [
                            "export_oaebu_table.book_product_publisher_metrics",
                            "create_oaebu_data_qa_onix_isbn",
                            "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country",
                            "export_oaebu_table.book_product_metrics_country",
                            "create_oaebu_data_qa_google_books_sales_isbn",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_analytics",
                            "export_oaebu_table.book_product_subject_year_metrics",
                            "export_oaebu_table.book_product_subject_thema_metrics",
                            "create_oaebu_data_qa_google_books_traffic_isbn",
                            "export_oaebu_table.book_product_subject_bic_metrics",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales",
                            "export_oaebu_table.book_product_list",
                            "export_oaebu_table.book_product_metrics_city",
                            "export_oaebu_table.book_product_author_metrics",
                            "export_oaebu_table.book_product_metrics_institution",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk",
                            "create_oaebu_data_qa_google_analytics_isbn",
                            "export_oaebu_table.book_product_year_metrics",
                            "export_oaebu_table.book_product_subject_bisac_metrics",
                            "create_oaebu_data_qa_onix_aggregate",
                            "export_oaebu_table.book_product_metrics_events",
                            "export_oaebu_table.institution_list",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic",
                            "create_oaebu_data_qa_oapen_irus_uk_isbn",
                            "export_oaebu_table.book_product_metrics",
                        ],
                        "create_oaebu_data_qa_onix_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_onix_aggregate": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_oapen_irus_uk_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_sales_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_traffic_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_analytics_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_analytics": [
                            "export_oaebu_qa_metrics"
                        ],
                        "export_oaebu_table.book_product_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_country": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_institution": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.institution_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_city": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_events": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_publisher_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bic_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bisac_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_thema_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_author_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_qa_metrics": ["cleanup"],
                        "cleanup": ["add_new_dataset_releases"],
                        "add_new_dataset_releases": [],
                    },
                    dag,
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("oaebu_workflows.workflows.onix_workflow.run_bigquery_query")
    @patch("oaebu_workflows.workflows.onix_workflow.bq_load_shard_v2")
    @patch("oaebu_workflows.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("oaebu_workflows.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
    def test_create_and_upload_bq_isbn13_workid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
        mock_mr,
        mock_api,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        mock_api.return_value = self.api

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )

                mock_mr.return_value = OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.datetime(2021, 1, 1),
                    onix_release_date=pendulum.datetime(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )

                release = wf.make_release()

                # Test works aggregation
                wf.aggregate_works(release)
                self.assertEqual(mock_write_to_file.call_count, 3)
                call_args = mock_write_to_file.call_args_list
                self.assertEqual(len(call_args[0][0][1]), 3)
                lookup_table = {arg["isbn13"]: arg["work_id"] for arg in call_args[0][0][1]}

                self.assertTrue(
                    lookup_table["111"] == lookup_table["112"] and lookup_table["112"] != lookup_table["211"]
                )

                self.assertEqual(call_args[0][0][0], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")
                self.assertEqual(
                    call_args[1][0][1],
                    [
                        {
                            "Error": "Product ISBN13:111 is a manifestation of ISBN13:113, which is not given as a product identifier in any ONIX product record."
                        },
                    ],
                )
                self.assertEqual(call_args[1][0][0], "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz")

                # Test upload_aggregation_tables
                wf.upload_aggregation_tables(release)
                self.assertEqual(mock_upload_files_from_list.call_count, 1)
                _, call_args = mock_upload_files_from_list.call_args
                self.assertEqual(call_args["bucket_name"], "bucket_name")
                self.assertEqual(
                    call_args["blob_names"],
                    [
                        "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    ],
                )
                self.assertEqual(
                    call_args["file_paths"],
                    [
                        "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    ],
                )

                # Test bq_load_workid_lookup
                wf.bq_load_workid_lookup(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 1)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(call_args["transform_blob"], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")
                self.assertEqual(call_args["dataset_id"], "onix_workflow")
                self.assertEqual(call_args["dataset_location"], "us")
                self.assertEqual(call_args["table_id"], "onix_workid_isbn")
                self.assertEqual(call_args["release_date"], pendulum.datetime(2021, 1, 1))
                self.assertEqual(call_args["source_format"], "NEWLINE_DELIMITED_JSON")
                self.assertEqual(call_args["dataset_description"], "ONIX workflow tables")

                # Test bq_load_workid_lookup_errors
                wf.bq_load_workid_lookup_errors(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 2)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(
                    call_args["transform_blob"], "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz"
                )
                self.assertEqual(call_args["dataset_id"], "onix_workflow")
                self.assertEqual(call_args["dataset_location"], "us")
                self.assertEqual(call_args["table_id"], "onix_workid_isbn_errors")
                self.assertEqual(call_args["release_date"], pendulum.datetime(2021, 1, 1))
                self.assertEqual(call_args["source_format"], "NEWLINE_DELIMITED_JSON")
                self.assertEqual(call_args["prefix"], "")
                self.assertEqual(call_args["schema_version"], "")
                self.assertEqual(call_args["dataset_description"], "ONIX workflow tables")

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("oaebu_workflows.workflows.onix_workflow.run_bigquery_query")
    @patch("oaebu_workflows.workflows.onix_workflow.bq_load_shard_v2")
    @patch("oaebu_workflows.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("oaebu_workflows.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
    def test_create_and_upload_bq_isbn13_workfamilyid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
        mock_mr,
        mock_api,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        mock_api.return_value = self.api

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )

                mock_mr.return_value = OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.datetime(2021, 1, 1),
                    onix_release_date=pendulum.datetime(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )
                release = wf.make_release()

                # Test works family aggregation
                wf.aggregate_works(release)
                self.assertEqual(mock_write_to_file.call_count, 3)
                call_args = mock_write_to_file.call_args_list
                lookup_table = {arg["isbn13"]: arg["work_family_id"] for arg in call_args[2][0][1]}
                self.assertTrue(
                    lookup_table["112"] == lookup_table["111"] and lookup_table["111"] == lookup_table["211"]
                )

                self.assertEqual(call_args[0][0][0], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")

                # Test bq_load_workfamilyid_lookup
                wf.upload_aggregation_tables(release)
                wf.bq_load_workfamilyid_lookup(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 1)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(
                    call_args["transform_blob"], "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz"
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    def test_create_oaebu_intermediate_table(self, mock_api):
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )
                with patch(
                    "oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query", return_value=False
                ) as _:
                    with patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset") as _:
                        self.assertRaises(
                            AirflowException,
                            wf.create_oaebu_intermediate_table,
                            release=OnixWorkflowRelease(
                                dag_id="onix_workflow_test",
                                release_date=pendulum.datetime(2021, 1, 1),
                                onix_release_date=pendulum.datetime(2021, 1, 1),
                                gcp_project_id=self.telescope.organisation.project_id,
                                gcp_bucket_name=self.bucket_name,
                                onix_dataset_id="onix",
                                onix_table_id="onix",
                            ),
                            orig_project_id="project",
                            orig_dataset="dataset",
                            orig_table="table",
                            orig_isbn="isbn",
                            sharded=False,
                        )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    @patch("oaebu_workflows.workflows.onix_workflow.select_table_shard_dates")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    def test_create_oaebu_intermediate_table_tasks(
        self,
        mock_select_table_shard_dates,
        mock_sel_table_suffixes,
        mock_create_bq_ds,
        mock_create_bq_table,
        mock_mr,
        mock_api,
    ):
        mock_api.return_value = self.api

        data_partners = [
            OaebuPartner(
                dataset_type_id="Test Partner",
                dag_id_prefix="test_dag",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table",
                isbn_field_name="isbn",
                title_field_name="title",
                sharded=True,
            ),
            OaebuPartner(
                dataset_type_id="Test Partner",
                dag_id_prefix="test_dag",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table2",
                isbn_field_name="isbn",
                title_field_name="title",
                sharded=True,
            ),
        ]

        mock_select_table_shard_dates.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_create_bq_table.return_value = True

        with self.env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    dag_id="dagid",
                    data_partners=data_partners,
                )

                mock_mr.return_value = OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.datetime(2021, 1, 1),
                    onix_release_date=pendulum.datetime(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )

                # Spin up tasks
                oaebu_task1_op = wf.operators[6]
                self.assertEqual(oaebu_task1_op[0].task_id, "create_oaebu_intermediate_table.test_dataset.test_table")
                self.assertEqual(oaebu_task1_op[1].task_id, "create_oaebu_intermediate_table.test_dataset.test_table2")

                # Run tasks
                oaebu_task1_op[0].execute_callable()
                _, call_args = mock_create_bq_ds.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["location"], "us")

                _, call_args = mock_create_bq_table.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["table_id"], "test_dataset_test_table_matched20210101")
                self.assertEqual(call_args["location"], "us")
                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "1dabe0b435be583fdbae6ad4421f579b"
                self.assertEqual(sql_hash, expected_hash)

                oaebu_task1_op[1].execute_callable()
                _, call_args = mock_create_bq_ds.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["location"], "us")

                _, call_args = mock_create_bq_table.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["table_id"], "test_dataset_test_table2_matched20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "200d285a02bdc14b3229874f50d8a30d"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_onix_isbn(self, mock_bq_ds, mock_bq_table_query, mock_api):
        mock_api.return_value = self.api

        with CliRunner().isolated_filesystem():
            with self.env.create():
                self.seed_db()
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_onix_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                )

                _, call_args = mock_bq_ds.call_args
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "onix_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "98d0a2b99961e9c63c3ade6faf9997f9"
                self.assertEqual(sql_hash, expected_hash)

                mock_bq_table_query.return_value = False
                self.assertRaises(
                    AirflowException,
                    wf.create_oaebu_data_qa_onix_isbn,
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_onix_aggregate(self, mock_bq_ds, mock_bq_table_query, mock_api):
        mock_api.return_value = self.api

        with CliRunner().isolated_filesystem():
            with self.env.create():
                self.seed_db()
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_onix_aggregate(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                )

                _, call_args = mock_bq_ds.call_args
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "onix_aggregate_metrics20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "e457bc4d32a3bbb75ef215009da917b3"
                self.assertEqual(sql_hash, expected_hash)

                mock_bq_table_query.return_value = False
                self.assertRaises(
                    AirflowException,
                    wf.create_oaebu_data_qa_onix_aggregate,
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_jstor_isbn(self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="jstor",
                    gcp_table_id="country",
                    isbn_field_name="ISBN",
                    title_field_name="Book_Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_jstor_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_jstor_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "jstor_invalid_eisbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "7a9a66b5a0295ecdd53d245e659f3e85"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_books_sales_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_api.return_value = self.api

        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_sales"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="google_books",
                    gcp_table_id="sales",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_google_books_sales_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="sales",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_google_books_sales_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="sales",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "google_books_sales_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "90bbe5c7fb00173d1a85f6ab13ab99b2"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_books_traffic_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)

            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_traffic"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="google_books",
                    gcp_table_id="traffic",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_google_books_traffic_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="traffic",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_google_books_traffic_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="sales",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "google_books_traffic_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "90bbe5c7fb00173d1a85f6ab13ab99b2"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_oapen_irus_uk_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="oapen_irus_uk",
                    gcp_project_id="project",
                    gcp_dataset_id="irus_uk",
                    gcp_table_id="oapen_irus_uk",
                    isbn_field_name="ISBN",
                    title_field_name="book_title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_oapen_irus_uk_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="irus_uk",
                    orig_table="oapen_irus_uk",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_oapen_irus_uk_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="irus_uk",
                    orig_table="oapen_irus_uk",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "oapen_irus_uk_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "ae842fbf661d3a0c50b748dec8e1cd24"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_analytics_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)

            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="google_analytics",
                    gcp_project_id="project",
                    gcp_dataset_id="google",
                    gcp_table_id="google_analytics",
                    isbn_field_name="publication_id",
                    title_field_name="book_title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_google_analytics_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google",
                    orig_table="google_analytics",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_google_analytics_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google",
                    orig_table="google_analytics",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "google_analytics_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "4302ca70501b435561f0c81d04e314cd"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_intermediate_unmatched_workid(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="jstor",
                    gcp_table_id="country",
                    isbn_field_name="ISBN",
                    title_field_name="Book_Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_intermediate_unmatched_workid(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    orig_isbn="isbn",
                    orig_title="Book_Title",
                )

                _, call_args = mock_bq_ds.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["table_id"], "country_unmatched_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "71bc329dd609017504e91bc8fd8931fe"
                self.assertEqual(sql_hash, expected_hash)

                mock_bq_table_query.return_value = False
                self.assertRaises(
                    AirflowException,
                    wf.create_oaebu_data_qa_intermediate_unmatched_workid,
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    orig_isbn="isbn",
                    orig_title="Book_Title",
                )


class TestOnixWorkflowFunctional(ObservatoryTestCase):
    """Functionally test the workflow.  No Google Analytics."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.timestamp = pendulum.now()
        self.schema_path = default_schema_folder()

        self.onix_table_id = "onix"
        self.test_onix_folder = random_id()  # "onix_workflow_test_onix_table"

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin Press"

    def seed_db(self):
        table_type_info = get_table_type_info()
        seed_table_type(table_type_info=table_type_info, api=self.api)

        dataset_type_info = get_dataset_type_info(self.api)
        seed_dataset_type(api=self.api, dataset_type_info=dataset_type_info)

    def setup_api(self, orgs=None):
        dt = pendulum.now("UTC")

        name = "Onix Workflow"
        workflow_type = WorkflowType(name=name, type_id=OnixWorkflow.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        workflow_type = WorkflowType(name="onix telescope", type_id="onix")
        self.api.put_workflow_type(workflow_type)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=OnixWorkflow.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        if orgs is None:
            orgs = [self.org_name]

        for org in orgs:
            organisation = Organisation(
                name=org,
                project_id="project",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
            )
            organisation = self.api.put_organisation(organisation)

            telescope = Workflow(
                name=name,
                workflow_type=WorkflowType(id=2),  # onix telescope
                organisation=Organisation(id=organisation.id),
                extra={},
            )
            self.api.put_workflow(telescope)

            telescope = Workflow(
                name=name,
                workflow_type=WorkflowType(id=1),
                organisation=Organisation(id=organisation.id),
                extra={},
            )
            telescope = self.api.put_workflow(telescope)

            dataset = Dataset(
                name="Onix Workflow Example Dataset",
                address="project.dataset.table",
                service="bigquery",
                workflow=Workflow(id=telescope.id),
                dataset_type=DatasetType(id=1),
            )
            self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_load(self):
        """Test that the DAG loads for each organisation"""

        dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "onix_workflow.py")
        org_names = ["ANU Press", "UCL Press", "University of Michigan Press"]

        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            # Add Observatory API connection
            self.setup_connections(env)
            self.setup_api(orgs=org_names)
            self.seed_db()

            # Check that all DAGs load
            for org_name in org_names:
                dag_id = make_dag_id("onix_workflow", org_name)
                self.assert_dag_load(dag_id, dag_file)

    def setup_fake_onix_data_table(self, dataset_id: str, settings_dataset_id: str, release_date: pendulum.DateTime):
        """Create a new onix data table with its own dataset id and table id, and populate it with some fake data."""

        # Upload fixture to bucket
        files = [test_fixtures_folder("onix_workflow", "onix.jsonl")]
        blobs = [os.path.join(self.test_onix_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=self.gcp_bucket_name, blob_names=blobs, file_paths=files)

        # Load into bigquery
        table_id, _ = table_ids_from_path("onix.jsonl")
        bq_load_shard_v2(
            self.schema_path,
            project_id=self.gcp_project_id,
            transform_bucket=self.gcp_bucket_name,
            transform_blob=blobs[0],
            dataset_id=dataset_id,
            dataset_location=self.data_location,
            table_id=table_id,
            release_date=release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description="Test Onix data for the workflow",
            **{},
        )

        country = load_jsonl(test_fixtures_folder("onix_workflow", "country.jsonl"))
        schema_path = test_fixtures_folder("onix_workflow", "schema")
        tables = [
            Table(
                "country",
                False,
                settings_dataset_id,
                country,
                "country",
                schema_path,
            ),
        ]

        bq_load_tables(
            tables=tables,
            bucket_name=self.gcp_bucket_name,
            release_date=release_date,
            data_location=self.data_location,
            project_id=self.gcp_project_id,
        )

    def setup_fake_partner_data(self, env, release_date: pendulum.DateTime):
        self.fake_partner_dataset = env.add_dataset()

        # Upload fixture to bucket
        path = test_fixtures_folder("onix_workflow")
        files = [
            os.path.join(path, "jstor_country.jsonl"),
            os.path.join(path, "jstor_institution.jsonl"),
            os.path.join(path, "google_books_sales.jsonl"),
            os.path.join(path, "google_books_traffic.jsonl"),
            os.path.join(path, "oapen_irus_uk.jsonl"),
            os.path.join(path, "google_analytics.jsonl"),
        ]
        blobs = [os.path.join(self.test_onix_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=self.gcp_bucket_name, blob_names=blobs, file_paths=files)

        # Load into bigquery
        table_ids = []
        for file_name, blob in zip(files, blobs):
            table_id, _ = table_ids_from_path(file_name)

            # set schema prefix to 'anu_press' for ANU press, custom dimensions are added in this schema.
            schema_prefix = "anu_press_" if table_id == "google_analytics" else ""

            bq_load_partition(
                self.schema_path,
                project_id=self.gcp_project_id,
                transform_bucket=self.gcp_bucket_name,
                transform_blob=blob,
                dataset_id=self.fake_partner_dataset,
                dataset_location=self.data_location,
                table_id=table_id,
                release_date=release_date,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition_type=bigquery.table.TimePartitioningType.MONTH,
                prefix=schema_prefix,
                dataset_description="Test Onix data for the workflow",
                partition_field="release_date",
                **{},
            )
            table_ids.append(table_id)

        self.seed_db()
        dataset_type_info = get_dataset_type_info(self.api)

        # Make partners
        partners = []
        for (dataset_type_id, isbn_field_name, title_field_name, dag_id_prefix), table_id in zip(
            [
                (dataset_type_info["jstor_country"].type_id, "ISBN", "Book_Title", "jstor"),
                (dataset_type_info["jstor_institution"].type_id, "ISBN", "Book_Title", "jstor"),
                (dataset_type_info["google_books_sales"].type_id, "Primary_ISBN", "Title", "google_books"),
                (dataset_type_info["google_books_traffic"].type_id, "Primary_ISBN", "Title", "google_books"),
                (dataset_type_info["oapen_irus_uk"].type_id, "ISBN", "book_title", "oapen_irus_uk"),
                (dataset_type_info["google_analytics"].type_id, "publication_id", "title", "google_analytics"),
            ],
            table_ids,
        ):
            partners.append(
                OaebuPartner(
                    dataset_type_id=dataset_type_id,
                    dag_id_prefix=dag_id_prefix,
                    gcp_project_id=self.gcp_project_id,
                    gcp_dataset_id=self.fake_partner_dataset,
                    gcp_table_id=table_id,
                    isbn_field_name=isbn_field_name,
                    title_field_name=title_field_name,
                    sharded=False,
                )
            )

        return partners

    def run_telescope_tests(self, *, org_name: str, include_google_analytics: bool = False):
        """Functional test of the ONIX workflow"""

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location, api_host=self.host, api_port=self.port)

        # Create datasets
        partner_release_date = pendulum.datetime(2022, 6, 13)
        onix_dataset_id = env.add_dataset(prefix="onix")
        oaebu_data_qa_dataset_id = env.add_dataset(prefix="oaebu_data_qa")
        onix_workflow_dataset_id = env.add_dataset(prefix="onix_workflow")
        oaebu_intermediate_dataset_id = env.add_dataset(prefix="oaebu_intermediate")
        oaebu_output_dataset_id = env.add_dataset(prefix="oaebu_output")
        oaebu_elastic_dataset_id = env.add_dataset(prefix="oaebu_elastic")
        oaebu_settings_dataset_id = env.add_dataset(prefix="settings")

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.gcp_bucket_name = env.transform_bucket
            self.setup_connections(env)
            self.setup_api(orgs=[org_name])
            # self.seed_db()

            # Setup data partners, remove Google Analytics (the last partner) from these tests
            data_partners = self.setup_fake_partner_data(env, partner_release_date)
            if not include_google_analytics:
                data_partners = data_partners[:-1]

            # Create fake data table. There's no guarantee the data was deleted so clean it again just in case.
            self.setup_fake_onix_data_table(onix_dataset_id, oaebu_settings_dataset_id, partner_release_date)

            # Pull info from Observatory API
            gcp_bucket_name = env.transform_bucket
            gcp_project_id = self.gcp_project_id

            # Expected sensor dag_ids
            sensor_dag_ids = ["onix"] + list(set([partner.dag_id_prefix for partner in data_partners]))
            sensor_dag_ids.sort()

            # Setup telescope
            start_date = pendulum.datetime(year=2021, month=5, day=9)
            telescope = OnixWorkflow(
                org_name=org_name,
                gcp_project_id=gcp_project_id,
                gcp_bucket_name=gcp_bucket_name,
                country_project_id=gcp_project_id,
                country_dataset_id=oaebu_settings_dataset_id,
                onix_dataset_id=onix_dataset_id,
                onix_table_id=self.onix_table_id,
                data_partners=data_partners,
                start_date=start_date,
                workflow_id=2,
            )

            # Skip dag existence check in sensor.
            for sensor in telescope.operators[0]:
                sensor.check_exists = False
                sensor.grace_period = timedelta(seconds=1)

            # Make DAG
            workflow_dag = telescope.make_dag()

            # If there is no dag run in the search interval, sensor will return success.
            expected_state = "success"
            with env.create_dag_run(workflow_dag, start_date):
                for task_id in sensor_dag_ids:
                    ti = env.run_task(f"{make_dag_id(task_id, org_name)}_sensor")
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            expected_state = "success"
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            release_date = pendulum.datetime(year=2021, month=5, day=22)
            for dag_id in sensor_dag_ids:
                dag = make_dummy_dag(make_dag_id(dag_id, org_name), execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual(expected_state, ti.state)

            # Run end to end tests for DOI DAG
            with env.create_dag_run(workflow_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in sensor_dag_ids:
                    ti = env.run_task(f"{make_dag_id(task_id, org_name)}_sensor")
                    self.assertEqual(expected_state, ti.state)

                # Mock make_release
                telescope.make_release = MagicMock(
                    return_value=OnixWorkflowRelease(
                        dag_id=make_dag_id(OnixWorkflow.DAG_ID_PREFIX, org_name),
                        release_date=release_date,
                        onix_release_date=partner_release_date,
                        gcp_project_id=self.gcp_project_id,
                        gcp_bucket_name=self.gcp_bucket_name,
                        onix_dataset_id=onix_dataset_id,
                        onix_table_id=self.onix_table_id,
                        oaebu_data_qa_dataset=oaebu_data_qa_dataset_id,
                        workflow_dataset=onix_workflow_dataset_id,
                        oaebu_intermediate_dataset=oaebu_intermediate_dataset_id,
                        oaebu_dataset=oaebu_output_dataset_id,
                        oaebu_elastic_dataset=oaebu_elastic_dataset_id,
                    )
                )

                # Aggregate works
                ti = env.run_task(telescope.aggregate_works.__name__)
                self.assertEqual(expected_state, ti.state)

                # Upload aggregation tables
                ti = env.run_task(telescope.upload_aggregation_tables.__name__)
                self.assertEqual(expected_state, ti.state)

                # Load work id table into bigquery
                ti = env.run_task(telescope.bq_load_workid_lookup.__name__)
                self.assertEqual(expected_state, ti.state)

                # Load work id errors table into bigquery
                ti = env.run_task(telescope.bq_load_workid_lookup_errors.__name__)
                self.assertEqual(expected_state, ti.state)

                # Load work family id table into bigquery
                ti = env.run_task(telescope.bq_load_workfamilyid_lookup.__name__)
                self.assertEqual(expected_state, ti.state)

                # Create oaebu intermediate tables
                for data_partner in data_partners:
                    oaebu_dataset = data_partner.gcp_dataset_id
                    oaebu_table = data_partner.gcp_table_id

                    ti = env.run_task(
                        f"{telescope.create_oaebu_intermediate_table.__name__}.{oaebu_dataset}.{oaebu_table}"
                    )
                    self.assertEqual(expected_state, ti.state)

                # Create oaebu output tables
                ti = env.run_task(telescope.create_oaebu_book_product_table.__name__)
                self.assertEqual(expected_state, ti.state)

                # ONIX isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_onix_isbn.__name__)
                self.assertEqual(expected_state, ti.state)

                # ONIX aggregate metrics
                ti = env.run_task(telescope.create_oaebu_data_qa_onix_aggregate.__name__)
                self.assertEqual(expected_state, ti.state)

                # JSTOR country isbn check
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_jstor_isbn.__name__}.{data_partners[0].gcp_dataset_id}.{data_partners[0].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # JSTOR country intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[0].gcp_dataset_id}.{data_partners[0].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # JSTOR institution isbn check
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_jstor_isbn.__name__}.{data_partners[1].gcp_dataset_id}.{data_partners[1].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # JSTOR institution intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[1].gcp_dataset_id}.{data_partners[1].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # Google Books Sales isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_google_books_sales_isbn.__name__)
                self.assertEqual(expected_state, ti.state)

                # Google Books Sales intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[2].gcp_dataset_id}.{data_partners[2].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # Google Books Traffic isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_google_books_traffic_isbn.__name__)
                self.assertEqual(expected_state, ti.state)

                # Google Books Traffic intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[3].gcp_dataset_id}.{data_partners[3].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # OAPEN IRUS UK isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_oapen_irus_uk_isbn.__name__)
                self.assertEqual(expected_state, ti.state)

                # OAPEN IRUS UK intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[4].gcp_dataset_id}.{data_partners[4].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                if include_google_analytics:
                    # Google Analytics isbn check
                    env.run_task(telescope.create_oaebu_data_qa_google_analytics_isbn.__name__)

                    # Google Books Analytics unmatched isbns
                    print("---------------------------------------")
                    print(f"{data_partners[5].gcp_dataset_id}.{data_partners[5].gcp_table_id}")
                    print("---------------------------------------")
                    env.run_task(
                        f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[5].gcp_dataset_id}.{data_partners[5].gcp_table_id}"
                    )

                # Export oaebu elastic tables
                export_tables = [
                    "book_product_list",
                    "book_product_metrics",
                    "book_product_metrics_country",
                    "book_product_metrics_institution",
                    "institution_list",
                    "book_product_metrics_city",
                    "book_product_metrics_events",
                    "book_product_publisher_metrics",
                    "book_product_subject_bic_metrics",
                    "book_product_subject_bisac_metrics",
                    "book_product_subject_thema_metrics",
                    "book_product_year_metrics",
                    "book_product_subject_year_metrics",
                    "book_product_author_metrics",
                ]

                for table in export_tables:
                    ti = env.run_task(f"{telescope.export_oaebu_table.__name__}.{table}")
                    self.assertEqual(expected_state, ti.state)

                # Export oaebu elastic qa table
                ti = env.run_task(telescope.export_oaebu_qa_metrics.__name__)
                self.assertEqual(expected_state, ti.state)

                # Test conditions
                release_suffix = release_date.strftime("%Y%m%d")

                transform_path = os.path.join(telescope.dag_id, release_suffix, "onix_workid_isbn.jsonl.gz")
                self.assert_blob_integrity(self.gcp_bucket_name, transform_path, transform_path)

                transform_path = os.path.join(telescope.dag_id, release_suffix, "onix_workfamilyid_isbn.jsonl.gz")
                self.assert_blob_integrity(self.gcp_bucket_name, transform_path, transform_path)

                transform_path = os.path.join(telescope.dag_id, release_suffix, "onix_workid_isbn_errors.jsonl.gz")
                self.assert_blob_integrity(self.gcp_bucket_name, transform_path, transform_path)

                table_id = f"{self.gcp_project_id}.{onix_workflow_dataset_id}.onix_workid_isbn{release_suffix}"
                self.assert_table_integrity(table_id, 3)

                table_id = f"{self.gcp_project_id}.{onix_workflow_dataset_id}.onix_workfamilyid_isbn{release_suffix}"
                self.assert_table_integrity(table_id, 3)

                table_id = f"{self.gcp_project_id}.{onix_workflow_dataset_id}.onix_workid_isbn_errors{release_suffix}"
                self.assert_table_integrity(table_id, 1)

                table_id = f"{self.gcp_project_id}.{oaebu_output_dataset_id}.book_product{release_suffix}"
                self.assert_table_integrity(table_id, 2)

                table_id = f"{self.gcp_project_id}.{oaebu_elastic_dataset_id}.{self.gcp_project_id.replace('-', '_')}_book_product_list{release_suffix}"
                self.assert_table_integrity(table_id, 2)

                table_id = f"{self.gcp_project_id}.{oaebu_elastic_dataset_id}.{self.gcp_project_id.replace('-', '_')}_book_product_publisher_metrics{release_suffix}"
                self.assert_table_integrity(table_id, 1)

                # Validate the joins worked
                # JSTOR
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.{self.fake_partner_dataset}_jstor_country_matched{release_suffix}"
                records = run_bigquery_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}

                self.assertTrue(
                    oaebu_works["111"] == oaebu_works["112"]
                    and oaebu_works["111"] != oaebu_works["211"]
                    and oaebu_works["113"] is None
                )

                self.assertTrue(
                    oaebu_wfam["111"] == oaebu_wfam["112"]
                    and oaebu_wfam["112"] == oaebu_wfam["211"]
                    and oaebu_wfam["113"] is None
                )

                # OAPEN IRUS UK
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.{self.fake_partner_dataset}_oapen_irus_uk_matched{release_suffix}"
                records = run_bigquery_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}

                self.assertTrue(
                    oaebu_works["111"] == oaebu_works["112"]
                    and oaebu_works["111"] != oaebu_works["211"]
                    and oaebu_works["113"] is None
                )

                self.assertTrue(
                    oaebu_wfam["111"] == oaebu_wfam["112"]
                    and oaebu_wfam["112"] == oaebu_wfam["211"]
                    and oaebu_wfam["113"] is None
                )

                # Check invalid ISBN13s picked up in ONIX
                sql = f"SELECT ISBN13 from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.onix_invalid_isbn{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["ISBN13"] for record in records])
                self.assertEqual(len(isbns), 3)
                self.assertTrue("211" in isbns)
                self.assertTrue("112" in isbns)
                self.assertTrue("111" in isbns)

                # Check ONIX aggregate metrics are correct
                sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.onix_aggregate_metrics{release_suffix}"
                records = run_bigquery_query(sql)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["table_size"], 3)
                self.assertEqual(records[0]["no_isbns"], 0)
                self.assertEqual(records[0]["no_relatedworks"], 0)
                self.assertEqual(records[0]["no_relatedproducts"], 2)
                self.assertEqual(records[0]["no_doi"], 3)
                self.assertEqual(records[0]["no_productform"], 1)
                self.assertEqual(records[0]["no_contributors"], 3)
                self.assertEqual(records[0]["no_titledetails"], 3)
                self.assertEqual(records[0]["no_publisher_urls"], 3)

                # Check JSTOR ISBN are valid
                sql = (
                    f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_isbn{release_suffix}"
                )
                records = run_bigquery_query(sql)
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("211" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)

                # Check JSTOR eISBN are valid
                sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_eisbn{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["eISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)
                self.assertTrue(None in isbns)

                # Check JSTOR unmatched ISBN picked up
                sql = f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_country_unmatched_ISBN{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 2)
                self.assertTrue("9781111111113" in isbns)
                self.assertTrue("113" in isbns)

                # Check OAPEN IRUS UK ISBN are valid
                sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.oapen_irus_uk_invalid_isbn{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)
                self.assertTrue("211" in isbns)

                # Check OAPEN IRUS UK unmatched ISBN picked up
                sql = f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.oapen_irus_uk_unmatched_ISBN{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 2)
                self.assertTrue("9781111111113" in isbns)
                self.assertTrue("113" in isbns)

                # Check Google Books Sales ISBN are valid
                sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_sales_invalid_isbn{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("211" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)

                # Check Google Books Sales unmatched ISBN picked up
                sql = f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_sales_unmatched_Primary_ISBN{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 2)
                self.assertTrue("9781111111113" in isbns)
                self.assertTrue("113" in isbns)

                # Check Google Books Traffic ISBN are valid
                sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_traffic_invalid_isbn{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("211" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)

                # Check Google Books Traffic unmatched ISBN picked up
                sql = f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_traffic_unmatched_Primary_ISBN{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 2)
                self.assertTrue("9781111111113" in isbns)
                self.assertTrue("113" in isbns)

                if include_google_analytics:
                    # Check Google Analytics ISBN are valid
                    sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_analytics_invalid_isbn{release_suffix}"
                    records = run_bigquery_query(sql)
                    isbns = set([record["publication_id"] for record in records])
                    self.assertEqual(len(isbns), 1)
                    self.assertTrue("(none)" in isbns)

                    # Check Google Analytics unmatched ISBN picked up
                    sql = f"SELECT publication_id from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_analytics_unmatched_publication_id{release_suffix}"
                    records = run_bigquery_query(sql)
                    isbns = set([record["publication_id"] for record in records])
                    self.assertEqual(len(isbns), 2)
                    self.assertTrue("9781111111113" in isbns)
                    self.assertTrue("(none)" in isbns)

                # Check Book Product Table
                sql = f"SELECT ISBN13 from {self.gcp_project_id}.{oaebu_output_dataset_id}.book_product{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["ISBN13"] for record in records])
                self.assertEqual(len(isbns), 2)
                self.assertTrue("211" in isbns)
                self.assertTrue("112" in isbns)

                # Check export tables
                sql = f"SELECT product_id from {self.gcp_project_id}.{oaebu_elastic_dataset_id}.{self.gcp_project_id.replace('-', '_')}_book_product_list{release_suffix}"
                records = run_bigquery_query(sql)
                isbns = set([record["product_id"] for record in records])
                self.assertEqual(len(isbns), 2)
                self.assertTrue("211" in isbns)
                self.assertTrue("112" in isbns)

                # Cleanup
                env.run_task(telescope.cleanup.__name__)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    def test_telescope(self):
        """Test that ONIX Workflow runs when Google Analytics is not included"""

        self.run_telescope_tests(org_name="Curtin Press", include_google_analytics=False)

    def test_telescope_with_google_analytics(self):
        """Test that ONIX Workflow runs when Google Analytics is included"""

        self.run_telescope_tests(org_name="ANU Press", include_google_analytics=True)
