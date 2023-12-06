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

# Author: Tuan Chien, Keegan Smith

import os
from datetime import timedelta
from unittest.mock import MagicMock, patch
import vcr
import shutil
from typing import List

import pendulum
from airflow.models import DagBag
from airflow.utils.state import State

from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from oaebu_workflows.onix_workflow.onix_workflow import (
    OnixWorkflow,
    OnixWorkflowRelease,
    CROSSREF_EVENT_URL_TEMPLATE,
    download_crossref_events,
    transform_crossref_events,
    transform_event,
    dois_from_table,
    download_crossref_event_url,
    create_latest_views_from_dataset,
    get_onix_records,
)
from observatory.platform.api import get_dataset_releases
from observatory.platform.observatory_config import Workflow
from observatory.platform.files import load_jsonl
from observatory.platform.bigquery import bq_find_schema, bq_run_query, bq_sharded_table_id, bq_table_id
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.config import module_file_path
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    find_free_port,
    random_id,
    make_dummy_dag,
    bq_load_tables,
    load_and_parse_json,
)


class TestOnixWorkflow(ObservatoryTestCase):
    """Functionally test the workflow"""

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.timestamp = pendulum.now()
        self.onix_table_id = "onix"
        self.test_onix_folder = random_id()  # "onix_workflow_test_onix_table"

        # For telescope Initialisation
        self.snapshot_date = pendulum.datetime(2021, 1, 1)
        self.dag_id = "onix_workflow_test"
        self.gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.fake_onix_data_partner = OaebuPartner(  # Onix data partner to pass to the telescope for initialisation
            type_id="irus_oapen",
            bq_dataset_id="test_dataset",
            bq_table_name="test_table",
            isbn_field_name="isbn",
            title_field_name="title",
            sharded=True,
            schema_path=os.path.join(default_schema_folder(workflow_module="onix_telescope"), "onix.json"),
        )

        # fixtures folder location
        self.fixtures_folder = test_fixtures_folder(workflow_module="onix_workflow")
        # vcrpy cassettes for http request mocking
        self.events_cassette = os.path.join(self.fixtures_folder, "crossref_events_request.yaml")

    @patch("oaebu_workflows.onix_workflow.onix_workflow.bq_select_table_shard_dates")
    def test_make_release(self, mock_sel_table_suffixes):
        """Tests that the make_release function works as intended"""
        # Use a different onix snapshot date for testing purposes
        onix_snapshot_date = self.snapshot_date.add(days=1)
        crossref_snapshot_date = self.snapshot_date
        mock_sel_table_suffixes.side_effect = [[onix_snapshot_date], [crossref_snapshot_date]]
        env = ObservatoryEnvironment(
            self.gcp_project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        with env.create():
            wf = OnixWorkflow(
                dag_id="test_make_release",
                cloud_workspace=self.fake_cloud_workspace,
                data_partners=[self.fake_onix_data_partner],
                metadata_partner="onix",
            )
            dag = wf.make_dag()
            with env.create_dag_run(dag, self.snapshot_date.add(days=1)):
                release = wf.make_release(
                    data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end)),
                    run_id=env.dag_run.run_id,
                )
                self.assertEqual(release.dag_id, wf.dag_id)

                # Test release file names are as expected
                self.assertEqual(release.workslookup_path, os.path.join(release.transform_folder, "worksid.jsonl.gz"))
                self.assertEqual(
                    release.workslookup_errors_path,
                    os.path.join(release.transform_folder, "worksid_errors.jsonl.gz"),
                )
                self.assertEqual(
                    release.worksfamilylookup_path, os.path.join(release.transform_folder, "workfamilyid.jsonl.gz")
                )
                self.assertEqual(
                    release.crossref_metadata_path,
                    os.path.join(release.transform_folder, "crossref_metadata.jsonl.gz"),
                )
                self.assertEqual(
                    release.crossref_events_path, os.path.join(release.transform_folder, "crossref_events.jsonl.gz")
                )

                # Test that the onix and crossref snapshots are as expected
                self.assertEqual(onix_snapshot_date, release.onix_snapshot_date)
                self.assertEqual(crossref_snapshot_date, release.crossref_master_snapshot_date)

                # Test for case - no ONIX releases found
                mock_sel_table_suffixes.side_effect = [[]]
                with self.assertRaisesRegex(AssertionError, "ONIX"):
                    release = wf.make_release(
                        data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end)),
                        run_id=env.dag_run.run_id,
                    )

                # Test for case - no Crossref releases found
                mock_sel_table_suffixes.side_effect = [[onix_snapshot_date], []]  # No crossref releases
                with self.assertRaisesRegex(AssertionError, "Crossref"):
                    release = wf.make_release(
                        data_interval_end=pendulum.parse(str(env.dag_run.data_interval_end)),
                        run_id=env.dag_run.run_id,
                    )

    def test_cleanup(self):
        """Tests the cleanup function of the workflow"""
        env = ObservatoryEnvironment(
            self.gcp_project_id,
            self.data_location,
            api_host="localhost",
            api_port=find_free_port(),
        )
        with env.create():
            wf = OnixWorkflow(
                dag_id="test_cleanup",
                cloud_workspace=self.fake_cloud_workspace,
                data_partners=[self.fake_onix_data_partner],
                metadata_partner="onix",
            )
            release = OnixWorkflowRelease(
                dag_id=wf.dag_id,
                run_id="test_run_id",
                snapshot_date=self.snapshot_date,
                onix_snapshot_date=self.snapshot_date,
                crossref_master_snapshot_date=self.snapshot_date,
            )
            self.assertTrue(os.path.exists(release.download_folder))
            self.assertTrue(os.path.exists(release.extract_folder))
            self.assertTrue(os.path.exists(release.transform_folder))
            wf.cleanup(release, execution_date=self.snapshot_date)
            self.assert_cleanup(release.workflow_folder)

    def test_dag_load(self):
        """Test that the DAG loads"""
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="onix_workflow_test_dag_load",
                    name="Onix Workflow Test Dag Load",
                    class_name="oaebu_workflows.onix_workflow.onix_workflow.OnixWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        sensor_dag_ids=[
                            "google_analytics3_test",
                            "google_books_sales_test",
                            "google_books_traffic_test",
                            "jstor_country_test",
                            "jstor_institution_test",
                            "irus_oapen_test",
                        ],
                        data_partners=[
                            "google_analytics3",
                            "google_books_sales",
                            "google_books_traffic",
                            "jstor_country",
                            "jstor_institution",
                            "irus_oapen",
                            "nonexistent_partner",
                        ],
                        metadata_partner="onix",
                    ),
                )
            ]
        )

        with env.create() as dag_folder:
            # This should raise one error for nonexistent partner
            shutil.copy(
                os.path.join(module_file_path("observatory.platform.dags"), "load_workflows.py"), "load_workflows.py"
            )
            dag_bag = DagBag(dag_folder=dag_folder)
            self.assertNotEqual({}, dag_bag.import_errors)
            self.assertEqual(len(dag_bag.import_errors), 1)

        # Remove the nonexistent partner and onix partner
        env.workflows[0].kwargs["data_partners"] = env.workflows[0].kwargs["data_partners"][:-1]
        with env.create():
            # Should not raise any errors
            self.assert_dag_load_from_config("onix_workflow_test_dag_load")

    def test_dag_structure(self):
        """Tests that the dag structure is created as expected on dag load"""
        env = ObservatoryEnvironment(
            self.gcp_project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        with env.create():
            data_partners = [
                "jstor_country",
                "irus_oapen",
                "google_books_sales",
                "google_books_traffic",
                "google_analytics3",
            ]
            sensor_dag_ids = ["jstor", "irus_oapen", "google_books", "onix", "google_analytics3"]
            dag = OnixWorkflow(
                dag_id=self.dag_id,
                cloud_workspace=self.fake_cloud_workspace,
                data_partners=data_partners,
                metadata_partner="onix",
                sensor_dag_ids=sensor_dag_ids,
            ).make_dag()
            expected_dag_structure = {
                "google_analytics3_sensor": ["aggregate_works"],
                "google_books_sensor": ["aggregate_works"],
                "jstor_sensor": ["aggregate_works"],
                "irus_oapen_sensor": ["aggregate_works"],
                "onix_sensor": ["aggregate_works"],
                "aggregate_works": ["upload_aggregation_tables"],
                "upload_aggregation_tables": ["bq_load_aggregations"],
                "bq_load_aggregations": ["create_oaebu_crossref_metadata_table"],
                "create_oaebu_crossref_metadata_table": ["create_oaebu_crossref_events_table"],
                "create_oaebu_crossref_events_table": ["create_oaebu_book_table"],
                "create_oaebu_book_table": [
                    "create_oaebu_intermediate_table_google_books_sales",
                    "create_oaebu_intermediate_table_irus_oapen",
                    "create_oaebu_intermediate_table_google_analytics3",
                    "create_oaebu_intermediate_table_google_books_traffic",
                    "create_oaebu_intermediate_table_jstor_country",
                ],
                "create_oaebu_intermediate_table_jstor_country": ["create_oaebu_book_product_table"],
                "create_oaebu_intermediate_table_irus_oapen": ["create_oaebu_book_product_table"],
                "create_oaebu_intermediate_table_google_books_sales": ["create_oaebu_book_product_table"],
                "create_oaebu_intermediate_table_google_books_traffic": ["create_oaebu_book_product_table"],
                "create_oaebu_intermediate_table_google_analytics3": ["create_oaebu_book_product_table"],
                "create_oaebu_book_product_table": [
                    "export_oaebu_table_book_product_publisher_metrics",
                    "create_oaebu_data_qa_isbn_onix",
                    "create_oaebu_data_qa_isbn_jstor_country",
                    "create_oaebu_data_qa_eisbn_jstor_country",
                    "create_oaebu_data_qa_intermediate_unmatched_jstor_country",
                    "export_oaebu_table_book_product_metrics_country",
                    "create_oaebu_data_qa_isbn_google_books_sales",
                    "create_oaebu_data_qa_intermediate_unmatched_google_analytics3",
                    "export_oaebu_table_book_product_subject_year_metrics",
                    "export_oaebu_table_book_product_subject_thema_metrics",
                    "create_oaebu_data_qa_isbn_google_books_traffic",
                    "export_oaebu_table_book_product_subject_bic_metrics",
                    "create_oaebu_data_qa_intermediate_unmatched_google_books_sales",
                    "export_oaebu_table_book_product_list",
                    "export_oaebu_table_book_product_metrics_city",
                    "export_oaebu_table_book_product_author_metrics",
                    "export_oaebu_table_book_product_metrics_institution",
                    "create_oaebu_data_qa_intermediate_unmatched_irus_oapen",
                    "create_oaebu_data_qa_isbn_google_analytics3",
                    "export_oaebu_table_book_product_year_metrics",
                    "export_oaebu_table_book_product_subject_bisac_metrics",
                    "create_oaebu_data_qa_onix_aggregate",
                    "export_oaebu_table_book_product_metrics_events",
                    "export_oaebu_table_institution_list",
                    "create_oaebu_data_qa_intermediate_unmatched_google_books_traffic",
                    "create_oaebu_data_qa_isbn_irus_oapen",
                    "export_oaebu_table_book_product_metrics",
                ],
                "create_oaebu_data_qa_isbn_onix": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_onix_aggregate": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_isbn_jstor_country": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_eisbn_jstor_country": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_intermediate_unmatched_jstor_country": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_isbn_irus_oapen": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_intermediate_unmatched_irus_oapen": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_isbn_google_books_sales": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_intermediate_unmatched_google_books_sales": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_isbn_google_books_traffic": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_intermediate_unmatched_google_books_traffic": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_isbn_google_analytics3": ["export_oaebu_qa_metrics"],
                "create_oaebu_data_qa_intermediate_unmatched_google_analytics3": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_list": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_metrics_country": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_metrics_institution": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_institution_list": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_metrics_city": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_metrics_events": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_publisher_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_subject_bic_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_subject_bisac_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_subject_thema_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_year_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_subject_year_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_table_book_product_author_metrics": ["export_oaebu_qa_metrics"],
                "export_oaebu_qa_metrics": ["create_oaebu_latest_views"],
                "create_oaebu_latest_views": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": [],
            }
            self.assert_dag_structure(expected_dag_structure, dag)

    @patch("oaebu_workflows.onix_workflow.onix_workflow.bq_run_query")
    def test_create_and_load_aggregate_works_table(self, mock_bq_query):
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        workslookup_expected = [
            {"isbn13": "112", "work_id": "111"},
            {"isbn13": "111", "work_id": "111"},
            {"isbn13": "211", "work_id": "211"},
        ]
        workslookup_errors_expected = [
            {
                "Error": "Product ISBN13:111 is a manifestation of ISBN13:113, which is not given as a product identifier in any ONIX product record."
            }
        ]
        worksfamilylookup_expected = [
            {"isbn13": "112", "work_family_id": "111"},
            {"isbn13": "111", "work_family_id": "111"},
            {"isbn13": "211", "work_family_id": "111"},
        ]
        env = ObservatoryEnvironment(
            self.gcp_project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        with env.create():
            wf = OnixWorkflow(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                data_partners=[self.fake_onix_data_partner],
                bq_onix_workflow_dataset=env.add_dataset(),
                metadata_partner="onix",
            )
            dag = wf.make_dag()
            with env.create_dag_run(dag, self.snapshot_date.add(days=1)):
                release = OnixWorkflowRelease(
                    dag_id="aggregation_test",
                    run_id=env.dag_run.run_id,
                    snapshot_date=self.snapshot_date,
                    onix_snapshot_date=self.snapshot_date,
                    crossref_master_snapshot_date=self.snapshot_date,
                )
                wf.aggregate_works(release)  # Test works aggregation
                wf.upload_aggregation_tables(release)  # Test upload_aggregation_tables
                wf.bq_load_aggregations(release, ti=MagicMock(task_id=""))  # Test bq_load_workid_lookup

                ### Make Assertions ###
                self.assertTrue(os.path.exists(release.workslookup_path))
                self.assertTrue(os.path.exists(release.workslookup_errors_path))
                self.assertTrue(os.path.exists(release.worksfamilylookup_path))

                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.workslookup_path),
                    release.workslookup_path,
                )
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.workslookup_errors_path),
                    release.workslookup_errors_path,
                )
                self.assert_blob_integrity(
                    env.transform_bucket,
                    gcs_blob_name_from_path(release.worksfamilylookup_path),
                    release.worksfamilylookup_path,
                )

                table_id = bq_sharded_table_id(
                    wf.cloud_workspace.project_id,
                    wf.bq_onix_workflow_dataset,
                    wf.bq_worksid_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, len(workslookup_expected))
                self.assert_table_content(table_id, load_jsonl(release.workslookup_path), primary_key="isbn13")
                self.assert_table_content(table_id, workslookup_expected, primary_key="isbn13")

                table_id = bq_sharded_table_id(
                    wf.cloud_workspace.project_id,
                    wf.bq_onix_workflow_dataset,
                    wf.bq_worksid_error_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, len(workslookup_errors_expected))
                self.assert_table_content(table_id, load_jsonl(release.workslookup_errors_path), primary_key="Error")
                self.assert_table_content(table_id, workslookup_errors_expected, primary_key="Error")

                table_id = bq_sharded_table_id(
                    wf.cloud_workspace.project_id,
                    wf.bq_onix_workflow_dataset,
                    wf.bq_workfamilyid_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, len(worksfamilylookup_expected))
                self.assert_table_content(table_id, load_jsonl(release.worksfamilylookup_path), primary_key="isbn13")
                self.assert_table_content(table_id, worksfamilylookup_expected, primary_key="isbn13")

    def test_crossref_API_calls(self):
        """
        Test the functions that query the crossref event and metadata APIs
        """
        # Test function calls with mocked HTTP responses
        good_test_doi = "10.5555/12345678"
        bad_test_doi = "10.1111111111111"
        mailto = "agent@observatory.academy"

        with vcr.use_cassette(
            os.path.join(self.fixtures_folder, "crossref_download_function_test.yaml"), record_mode="none"
        ):
            events_start = pendulum.date(2020, 1, 1)
            events_end = pendulum.date(2022, 1, 1)
            event_url = CROSSREF_EVENT_URL_TEMPLATE.format(
                doi=good_test_doi,
                mailto=mailto,
                start_date=events_start.strftime("%Y-%m-%d"),
                end_date=events_end.strftime("%Y-%m-%d"),
            )
            events = download_crossref_event_url(event_url)
            assert events == [{"passed": True}], f"Event return incorrect. Got {events}"

        good_events = download_crossref_events([good_test_doi], events_start, events_end, mailto, max_threads=1)
        bad_events = download_crossref_events([bad_test_doi], events_start, events_end, mailto, max_threads=1)
        assert good_events, "Events should have returned something"
        assert len(good_events) == 4
        assert not bad_events, f"Events should have returned nothing, instead returned {bad_events}"

    def test_crossref_transform(self):
        """Test the function that transforms the crossref events data"""

        input_events = [
            {
                "license": "https://creativecommons.org/publicdomain/zero/1.0/",
                "obj_id": "https://doi.org/10.5555/12345678",
                "source_token": "36c35e23-8757-4a9d-aacf-345e9b7eb50d",
                "occurred_at": "2021-07-21T13:27:50Z",
                "subj_id": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                "id": "c76bbfd9-5122-4859-82c8-505b1eb845fd",
                "evidence_record": "https://evidence.eventdata.crossref.org/evidence/20210721-wikipedia-83789066-1794-4c4f-bb2c-ed47ccd82264",
                "terms": "https://doi.org/10.13003/CED-terms-of-use",
                "action": "add",
                "subj": {
                    "pid": "https://en.wikipedia.org/wiki/Josiah_S._Carberry",
                    "url": "https://en.wikipedia.org/w/index.php?title=Josiah_S._Carberry&oldid=1034726541",
                    "title": "Josiah S. Carberry",
                    "work_type_id": "entry-encyclopedia",
                    "api-url": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                },
                "source_id": "wikipedia",
                "obj": {
                    "pid": "https://doi.org/10.5555/12345678",
                    "url": "http://psychoceramics.labs.crossref.org/10.5555-12345678.html",
                    "method": "landing-page-meta-tag",
                    "verification": "checked-url-exact",
                },
                "timestamp": "2021-07-21T14:00:58Z",
                "relation_type_id": "references",
            }
        ]
        expected_transformed_events = [
            {
                "license": "https://creativecommons.org/publicdomain/zero/1.0/",
                "obj_id": "https://doi.org/10.5555/12345678",
                "source_token": "36c35e23-8757-4a9d-aacf-345e9b7eb50d",
                "occurred_at": "2021-07-21T13:27:50+00:00",
                "subj_id": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                "id": "c76bbfd9-5122-4859-82c8-505b1eb845fd",
                "evidence_record": "https://evidence.eventdata.crossref.org/evidence/20210721-wikipedia-83789066-1794-4c4f-bb2c-ed47ccd82264",
                "terms": "https://doi.org/10.13003/CED-terms-of-use",
                "action": "add",
                "subj": {
                    "pid": "https://en.wikipedia.org/wiki/Josiah_S._Carberry",
                    "url": "https://en.wikipedia.org/w/index.php?title=Josiah_S._Carberry&oldid=1034726541",
                    "title": "Josiah S. Carberry",
                    "work_type_id": "entry-encyclopedia",
                    "api_url": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                },
                "source_id": "wikipedia",
                "obj": {
                    "pid": "https://doi.org/10.5555/12345678",
                    "url": "http://psychoceramics.labs.crossref.org/10.5555-12345678.html",
                    "method": "landing-page-meta-tag",
                    "verification": "checked-url-exact",
                },
                "timestamp": "2021-07-21T14:00:58+00:00",
                "relation_type_id": "references",
            }
        ]
        # Standalone transform
        actual_transformed_events = transform_event(input_events[0])
        assert expected_transformed_events[0] == actual_transformed_events
        # List transform
        actual_transformed_events = transform_crossref_events(input_events)
        assert len(actual_transformed_events) == 1
        assert expected_transformed_events == actual_transformed_events

    def test_utility_functions(self):
        """
        Test the standalone functions in the Onix workflow that aren't specifially tested in other classes
        """

        ############################
        ### Test dois_from_table ###
        ############################

        env = ObservatoryEnvironment(
            self.gcp_project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        fake_doi_isbn_dataset_id = env.add_dataset(prefix="doi_isbn_test")
        fake_sharded_dataset = env.add_dataset(prefix="sharded_data")
        fake_view_dataset = env.add_dataset(prefix="views")
        fake_table_schema = os.path.join(default_schema_folder(workflow_module="onix_telescope"), "onix.json")
        with env.create():
            doi_table_file = os.path.join(self.fixtures_folder, "doi_isbn_query_test.jsonl")

            fake_doi_isbn_table = load_jsonl(doi_table_file)
            test_table = Table(
                "doi_isbn_test",
                False,
                fake_doi_isbn_dataset_id,
                fake_doi_isbn_table,
                fake_table_schema,  # use onix schema just for ease uploading
            )

            release_date = pendulum.datetime(2022, 6, 13)
            bq_load_tables(
                tables=[test_table],
                bucket_name=env.transform_bucket,
                snapshot_date=release_date,
                project_id=self.gcp_project_id,
            )
            table_id = bq_table_id(self.gcp_project_id, fake_doi_isbn_dataset_id, "doi_isbn_test")
            actual_dois = dois_from_table(table_id, doi_column_name="DOI", distinct=True)
            fake_doi_isbns = [entry["DOI"] for entry in fake_doi_isbn_table]

            # Check there are no duplicates and the contents are the same
            assert len(actual_dois) == len(set(fake_doi_isbns))
            assert set(actual_dois) == set(fake_doi_isbns)

            # Do the same but allow duplicates
            actual_dois = dois_from_table(table_id, doi_column_name="DOI", distinct=False)
            fake_doi_isbns = [entry["DOI"] for entry in fake_doi_isbn_table]
            assert len(actual_dois) == len(fake_doi_isbns)
            assert sorted(actual_dois) == sorted(fake_doi_isbns)

            #############################################
            ### Test create_latest_views_from_dataset ###
            #############################################

            # We will use the onix.jsonl fixture for testing.
            previous_release_date = pendulum.datetime(2022, 6, 6)  # Add another release date
            data = [fake_doi_isbn_table, fake_doi_isbn_table[:2]]
            for release, table in zip([release_date, previous_release_date], data):
                test_table = Table("data_export", True, fake_sharded_dataset, table, fake_table_schema)
                bq_load_tables(
                    tables=[test_table],
                    bucket_name=env.transform_bucket,
                    snapshot_date=release,
                    project_id=self.gcp_project_id,
                )

            # Now make the views from this dataset
            create_latest_views_from_dataset(
                project_id=self.gcp_project_id,
                from_dataset=fake_sharded_dataset,
                to_dataset=fake_view_dataset,
                date_match=release_date.strftime("%Y%m%d"),
                data_location=self.data_location,
            )

            # Grab the data from the view and make assertions
            view_data = bq_run_query(f"SELECT * FROM {self.gcp_project_id}.{fake_view_dataset}.data_export")
            view_isbns = [entry["ISBN13"] for entry in view_data]
            view_dois = [entry["DOI"] for entry in view_data]
            actual_isbns = [entry["ISBN13"] for entry in fake_doi_isbn_table]
            actual_dois = [entry["DOI"] for entry in fake_doi_isbn_table]

            assert len(view_data) == len(fake_doi_isbn_table)
            assert len(actual_isbns) == len(view_isbns)
            assert sorted(actual_isbns) == sorted(view_isbns)
            assert len(actual_dois) == len(view_dois)
            assert sorted(actual_dois) == sorted(view_dois)

    def setup_fake_lookup_tables(
        self, settings_dataset_id: str, fixtures_dataset_id: str, release_date: pendulum.DateTime, bucket_name: str
    ):
        """Create a new onix and subject lookup and country tables with their own dataset and table ids.
        Populate them with some fake data.

        :param settings_dataset_id: The dataset to store the country table
        :param fixtures_dataset_id: The dataset to store the lookup tables
        :param release_date: The release/snapshot date
        :param bucket_name: The GCP bucket name to load the data to
        """

        table_and_dataset = [
            ("bic_lookup", fixtures_dataset_id),
            ("bisac_lookup", fixtures_dataset_id),
            ("thema_lookup", fixtures_dataset_id),
            ("country", settings_dataset_id),
        ]

        # Create the table objects
        onix_test_schema_folder = os.path.join(self.fixtures_folder, "schema")
        tables = []
        for table_name, dataset in table_and_dataset:
            tables.append(
                Table(
                    table_name,
                    False,
                    dataset,
                    load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", f"{table_name}.jsonl")),
                    bq_find_schema(path=onix_test_schema_folder, table_name=table_name),
                )
            )

        # Load tables into bigquery
        bq_load_tables(
            tables=tables, bucket_name=bucket_name, snapshot_date=release_date, project_id=self.gcp_project_id
        )

    def setup_input_data(
        self,
        settings_dataset_id: str,
        fixtures_dataset_id: str,
        crossref_master_dataset_id: str,
        partner_dataset: str,
        onix_dataset_id: str,
        release_date: pendulum.DateTime,
        bucket_name: str,
        include_google_analytics3: bool,
    ) -> List[OaebuPartner]:
        """Uploads the data partner fixtures to their respective GCP bucket and bigquery tables.
        Creates the partners based on the originals - but changes the dataset ids for tests.
        Create a new onix and subject lookup and country tables with their own dataset and table ids.

        :param settings_dataset_id: The dataset to store the country table
        :param fixtures_dataset_id: The dataset to store the lookup tables
        :param partner_dataset: The bigquery dataset ID to load the data partner tables to
        :param crossref_master_dataset_id: The bigquery dataset ID of the master crossref table
        :param onix_dataaset: The Bigquery dataset ID to load the onix partner table to
        :param release_date: The release/snapshot date of sharded tables
        :param bucket_name: The name of the bucket to upload the jsonl files to
        :param include_google_analytics3: Whether to include google analytics 3 as a partner
        :return: The resulting OaebuPartners
        """

        ############################################
        ### Upload lookups, country and crossref ###
        ############################################

        table_and_dataset = [
            ("bic_lookup", fixtures_dataset_id, False),
            ("bisac_lookup", fixtures_dataset_id, False),
            ("thema_lookup", fixtures_dataset_id, False),
            ("country", settings_dataset_id, False),
            ("crossref_metadata_master", crossref_master_dataset_id, True),
        ]

        # Create the table objects
        onix_test_schema_folder = os.path.join(self.fixtures_folder, "schema")
        tables = []
        for table_name, dataset, sharded in table_and_dataset:
            tables.append(
                Table(
                    table_name,
                    sharded,
                    dataset,
                    load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", f"{table_name}.jsonl")),
                    bq_find_schema(path=onix_test_schema_folder, table_name=table_name),
                )
            )

        ###########################
        ### Upload partner data ###
        ###########################

        table_dataset_sharded_schema = [
            ("jstor_country", partner_dataset),
            ("jstor_institution", partner_dataset),
            ("google_books_sales", partner_dataset),
            ("google_books_traffic", partner_dataset),
            ("irus_oapen", partner_dataset),
            ("irus_fulcrum", partner_dataset),
            ("ucl_discovery", partner_dataset),
        ]
        if include_google_analytics3:
            table_dataset_sharded_schema.append(("google_analytics3", partner_dataset))

        # Create partner objects define tables
        partners = []
        for partner_name, dataset in table_dataset_sharded_schema:
            partner = partner_from_str(partner_name)
            partner.bq_dataset_id = dataset
            partners.append(partner)
            tables.append(
                Table(
                    partner_name,
                    partner.sharded,
                    partner.bq_dataset_id,
                    load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", f"{partner_name}.jsonl")),
                    partner.schema_path,
                )
            )

        # Create metadata table
        metadata_partner = partner_from_str("onix", metadata_partner=True)
        metadata_partner.bq_dataset_id = onix_dataset_id
        tables.append(
            Table(
                "onix",
                metadata_partner.sharded,
                metadata_partner.bq_dataset_id,
                load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", f"onix.jsonl")),
                metadata_partner.schema_path,
            )
        )

        # Load tables into bigquery
        bq_load_tables(
            tables=tables, bucket_name=bucket_name, snapshot_date=release_date, project_id=self.gcp_project_id
        )

        return partners, metadata_partner

    def run_telescope_tests(self, *, include_google_analytics3: bool = False):
        """Functional test of the ONIX workflow"""

        def vcr_ignore_condition(request):
            """This function is used by vcrpy to allow requests to sources not in the cassette file.
            At time of writing, the only mocked requests are the ones to crossref events."""
            allowed_domains = ["https://api.eventdata.crossref.org"]
            allow_request = any([request.url.startswith(i) for i in allowed_domains])
            if not allow_request:
                return None
            return request

        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.gcp_project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )

        # Create workflow datasets
        onix_workflow_dataset_id = env.add_dataset(prefix="onix_workflow")
        master_crossref_dataset_id = env.add_dataset(prefix="crossref_master")
        oaebu_data_qa_dataset_id = env.add_dataset(prefix="oaebu_data_qa")
        oaebu_latest_data_qa_dataset_id = env.add_dataset(prefix="oaebu_data_qa_latest")
        oaebu_intermediate_dataset_id = env.add_dataset(prefix="oaebu_intermediate")
        oaebu_output_dataset_id = env.add_dataset(prefix="oaebu_output")
        oaebu_export_dataset_id = env.add_dataset(prefix="oaebu_export")
        oaebu_latest_export_dataset_id = env.add_dataset(prefix="oaebu_export_latest")
        oaebu_settings_dataset_id = env.add_dataset(prefix="settings")
        oaebu_fixtures_dataset_id = env.add_dataset(prefix="fixtures")
        oaebu_crossref_dataset_id = env.add_dataset(prefix="crossref")

        # Fake partner datasets
        onix_dataset_id = env.add_dataset(prefix="onix")
        fake_partner_dataset_id = env.add_dataset(prefix="partner")

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            # Setup data partners, remove Google Analytics (the last partner) from these tests
            partner_release_date = pendulum.datetime(2021, 5, 15)
            data_partners, metadata_partner = self.setup_input_data(
                settings_dataset_id=oaebu_settings_dataset_id,
                fixtures_dataset_id=oaebu_fixtures_dataset_id,
                crossref_master_dataset_id=master_crossref_dataset_id,
                partner_dataset=fake_partner_dataset_id,
                onix_dataset_id=onix_dataset_id,
                release_date=partner_release_date,
                bucket_name=env.transform_bucket,
                include_google_analytics3=include_google_analytics3,
            )

            # Expected sensor dag_ids
            sensor_dag_ids = [
                "jstor",
                "google_books",
                "google_analytics3",
                "irus_oapen",
                "irus_fulcrum",
                "ucl_discovery",
                "onix",
            ]

            # Setup telescope
            telescope = OnixWorkflow(
                dag_id=f"onix_workflow_test",
                cloud_workspace=env.cloud_workspace,
                metadata_partner=metadata_partner,
                bq_master_crossref_project_id=env.cloud_workspace.project_id,
                bq_master_crossref_dataset_id=master_crossref_dataset_id,
                bq_oaebu_crossref_dataset_id=oaebu_crossref_dataset_id,
                bq_master_crossref_metadata_table_name="crossref_metadata_master",  # Set in setup_input_data()
                bq_country_project_id=env.cloud_workspace.project_id,
                bq_country_dataset_id=oaebu_settings_dataset_id,
                bq_subject_project_id=env.cloud_workspace.project_id,
                bq_subject_dataset_id=oaebu_fixtures_dataset_id,
                bq_oaebu_data_qa_dataset=oaebu_data_qa_dataset_id,
                bq_oaebu_latest_data_qa_dataset=oaebu_latest_data_qa_dataset_id,
                bq_onix_workflow_dataset=onix_workflow_dataset_id,
                bq_oaebu_intermediate_dataset=oaebu_intermediate_dataset_id,
                bq_oaebu_dataset=oaebu_output_dataset_id,
                bq_oaebu_export_dataset=oaebu_export_dataset_id,
                bq_oaebu_latest_export_dataset=oaebu_latest_export_dataset_id,
                data_partners=data_partners,
                sensor_dag_ids=sensor_dag_ids,
                start_date=pendulum.datetime(year=2021, month=5, day=9),
                crossref_start_date=pendulum.datetime(year=2018, month=5, day=14),
                max_threads=1,  # Use 1 thread for tests
            )

            # Skip dag existence check in sensor.
            for sensor in telescope.operators[0]:
                sensor.check_exists = False
                sensor.grace_period = timedelta(seconds=1)
            workflow_dag = telescope.make_dag()

            # If there is no dag run in the search interval, sensor will return success.
            expected_state = "success"
            with env.create_dag_run(workflow_dag, telescope.start_date):
                for sensor_id in sensor_dag_ids:
                    ti = env.run_task(f"{sensor_id}_sensor")
                    self.assertEqual(ti.state, State.SUCCESS)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            for dag_id in sensor_dag_ids:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual(ti.state, State.SUCCESS)

            # Run end to end tests for DOI DAG
            with env.create_dag_run(workflow_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for dag_id in sensor_dag_ids:
                    ti = env.run_task(f"{dag_id}_sensor")
                    self.assertEqual(ti.state, State.SUCCESS)

                # Mock make_release
                release_date = pendulum.datetime(year=2021, month=5, day=22)
                telescope.make_release = MagicMock(
                    return_value=OnixWorkflowRelease(
                        dag_id=telescope.dag_id,
                        run_id=env.dag_run.run_id,
                        snapshot_date=release_date,
                        onix_snapshot_date=partner_release_date,
                        crossref_master_snapshot_date=partner_release_date,
                    )
                )
                release = telescope.make_release()

                # Make the SQL queries for data QA
                release_suffix = release.snapshot_date.strftime("%Y%m%d")
                data_qa_sql = {
                    "onix_invalid_isbn": f"SELECT ISBN13 from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.onix_invalid_isbn{release_suffix}",
                    "onix_aggregate_metrics": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.onix_aggregate_metrics{release_suffix}",
                    "jstor_country_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_isbn{release_suffix}",
                    "jstor_country_invalid_eisbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_eisbn{release_suffix}",
                    "jstor_country_unmatched_ISBN": f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_country_unmatched_ISBN{release_suffix}",
                    "jstor_institution_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_isbn{release_suffix}",
                    "jstor_institution_invalid_eisbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_eisbn{release_suffix}",
                    "jstor_institution_unmatched_ISBN": f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_institution_unmatched_ISBN{release_suffix}",
                    "google_books_sales_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_sales_invalid_isbn{release_suffix}",
                    "google_books_sales_unmatched_Primary_ISBN": f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_sales_unmatched_Primary_ISBN{release_suffix}",
                    "google_books_traffic_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_traffic_invalid_isbn{release_suffix}",
                    "google_books_traffic_unmatched_Primary_ISBN": f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_traffic_unmatched_Primary_ISBN{release_suffix}",
                    "irus_oapen_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.irus_oapen_invalid_isbn{release_suffix}",
                    "irus_oapen_unmatched_ISBN": f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.irus_oapen_unmatched_ISBN{release_suffix}",
                    "irus_fulcrum_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.irus_fulcrum_invalid_isbn{release_suffix}",
                    "irus_fulcrum_unmatched_ISBN": f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.irus_fulcrum_unmatched_ISBN{release_suffix}",
                    "ucl_discovery_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.ucl_discovery_invalid_isbn{release_suffix}",
                    "ucl_discovery_unmatched_ISBN": f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.ucl_discovery_unmatched_ISBN{release_suffix}",
                }
                if include_google_analytics3:
                    data_qa_sql[
                        "google_analytics3_invalid_isbn"
                    ] = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_analytics3_invalid_isbn{release_suffix}"

                    data_qa_sql[
                        "google_analytics3_unmatched_publication_id"
                    ] = f"SELECT publication_id from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_analytics3_unmatched_publication_id{release_suffix}"

                # Create the latest data QA SQL as well
                latest_data_qa_sql = {}
                for k, sql in data_qa_sql.items():
                    latest_data_qa_sql[k] = sql.replace(
                        oaebu_data_qa_dataset_id, oaebu_latest_data_qa_dataset_id
                    ).replace(release_suffix, "")

                # Aggregate works
                ti = env.run_task(telescope.aggregate_works.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Upload aggregation tables
                ti = env.run_task(telescope.upload_aggregation_tables.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Load work id, work id errors and work family id tables into bigquery
                ti = env.run_task(telescope.bq_load_aggregations.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_sharded_table_id(
                    self.gcp_project_id, onix_workflow_dataset_id, telescope.bq_worksid_table_name, release_date
                )
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "onix_workid_isbn.json")),
                    primary_key="isbn13",
                )
                table_id = bq_sharded_table_id(
                    self.gcp_project_id, onix_workflow_dataset_id, telescope.bq_worksid_error_table_name, release_date
                )
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(
                        os.path.join(self.fixtures_folder, "e2e_outputs", "onix_workid_isbn_errors.json")
                    ),
                    primary_key="Error",
                )
                table_id = bq_sharded_table_id(
                    self.gcp_project_id, onix_workflow_dataset_id, telescope.bq_workfamilyid_table_name, release_date
                )
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(
                        os.path.join(self.fixtures_folder, "e2e_outputs", "onix_workfamilyid_isbn.json")
                    ),
                    primary_key="isbn13",
                )

                # Load crossref metadata table into bigquery
                ti = env.run_task(telescope.create_oaebu_crossref_metadata_table.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Assertions
                table_id = bq_sharded_table_id(
                    self.gcp_project_id,
                    oaebu_crossref_dataset_id,
                    telescope.bq_oaebu_crossref_metadata_table_name,
                    release_date,
                )
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "crossref_metadata.json")),
                    primary_key="DOI",
                )

                # Load crossref event table into bigquery
                with vcr.use_cassette(
                    self.events_cassette, record_mode="none", before_record_request=vcr_ignore_condition
                ):
                    ti = env.run_task(telescope.create_oaebu_crossref_events_table.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_sharded_table_id(
                    self.gcp_project_id,
                    oaebu_crossref_dataset_id,
                    telescope.bq_crossref_events_table_name,
                    release_date,
                )
                crossref_fixture_table = load_and_parse_json(
                    os.path.join(self.fixtures_folder, "e2e_outputs", "crossref_events.json"),
                    timestamp_fields=["timestamp", "occurred_at", "updated_date", "issued", "dateModified"],
                )
                self.assert_table_content(table_id, crossref_fixture_table, primary_key="id")

                # Create book table in bigquery
                ti = env.run_task(telescope.create_oaebu_book_table.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_sharded_table_id(
                    self.gcp_project_id, oaebu_output_dataset_id, telescope.bq_book_table_name, release_date
                )
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "book.json")),
                    primary_key="isbn",
                )

                # Create oaebu intermediate tables - onix doesn't have an intermediate table so skip it
                for data_partner in data_partners:
                    ti = env.run_task(f"create_oaebu_intermediate_table_{data_partner.bq_table_name}")
                    self.assertEqual(ti.state, State.SUCCESS)

                # Create oaebu output tables
                ti = env.run_task(telescope.create_oaebu_book_product_table.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                fixture_name = "book_product_ga.json" if include_google_analytics3 else "book_product.json"
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_sharded_table_id(
                    self.gcp_project_id, oaebu_output_dataset_id, telescope.bq_book_product_table_name, release_date
                )
                self.assert_table_content(
                    table_id,
                    load_and_parse_json(
                        os.path.join(self.fixtures_folder, "e2e_outputs", fixture_name),
                        date_fields={"month", "published_date"},
                    ),
                    primary_key="ISBN13",
                )

                ############################################################
                ### Check the QA + unmatched tasks for each data partner ###
                ############################################################

                # Check ONIX invalid ISBN13s picked up in ONIX
                ti = env.run_task(telescope.create_oaebu_data_qa_isbn_onix.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                records = bq_run_query(data_qa_sql["onix_invalid_isbn"])
                isbns = set([record["ISBN13"] for record in records])
                self.assertEqual(len(isbns), 1)
                self.assertIn("1111111111111", isbns)

                # ONIX aggregate metrics and check aggregate metrics are correct
                ti = env.run_task(telescope.create_oaebu_data_qa_onix_aggregate.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                records = bq_run_query(data_qa_sql["onix_aggregate_metrics"])
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["table_size"], 4)
                self.assertEqual(records[0]["no_isbns"], 0)
                self.assertEqual(records[0]["no_relatedworks"], 0)
                self.assertEqual(records[0]["no_relatedproducts"], 3)
                self.assertEqual(records[0]["no_doi"], 4)
                self.assertEqual(records[0]["no_productform"], 0)
                self.assertEqual(records[0]["no_contributors"], 2)
                self.assertEqual(records[0]["no_titledetails"], 4)
                self.assertEqual(records[0]["no_publisher_urls"], 4)

                task_id_qa_isbn_fmt = "create_oaebu_data_qa_isbn_{data_partner_table_name}"
                task_id_qa_eisbn_fmt = "create_oaebu_data_qa_eisbn_{data_partner_table_name}"
                task_id_qa_unmatched_fmt = "create_oaebu_data_qa_intermediate_unmatched_{data_partner_table_name}"
                invalid_isbns_expected = set(sorted(["111", "1111111111111", "113", "211"]))
                invalid_eisbns_expected = set(
                    sorted(["111", "1111111111111", "113", None], key=lambda x: x or "0")
                )  # lambda method allows sorting a None
                unmatched_isbns_expected = set(sorted(["111", "113", "211"]))

                for partner in data_partners:
                    # Run ISBN QA task
                    ti = env.run_task(task_id_qa_isbn_fmt.format(data_partner_table_name=partner.bq_table_name))
                    self.assertEqual(ti.state, State.SUCCESS)
                    # Run unatched QA task
                    ti = env.run_task(task_id_qa_unmatched_fmt.format(data_partner_table_name=partner.bq_table_name))
                    self.assertEqual(ti.state, State.SUCCESS)

                    if partner.type_id.startswith("google_analytics3"):  # GA is a special case
                        # Check ISBNs are valid
                        records = bq_run_query(data_qa_sql["google_analytics3_invalid_isbn"])
                        isbns = set([record["publication_id"] for record in records])
                        self.assertEqual(len(isbns), 1)
                        self.assertTrue("(none)" in isbns)
                        # Check unmatched ISBNs picked up
                        records = bq_run_query(data_qa_sql["google_analytics3_unmatched_publication_id"])
                        isbns = set([record["publication_id"] for record in records])
                        self.assertEqual(len(isbns), 1)
                        self.assertTrue("(none)" in isbns)
                    else:
                        # Check ISBNs are valid
                        records = bq_run_query(data_qa_sql[f"{partner.type_id}_invalid_isbn"])
                        isbns = set(sorted([record[partner.isbn_field_name] for record in records]))
                        self.assertEqual(isbns, invalid_isbns_expected)
                        # Check unmatched ISBNs picked up
                        records = bq_run_query(
                            data_qa_sql[f"{partner.bq_table_name}_unmatched_{partner.isbn_field_name}"]
                        )
                        isbns = set(sorted([record[partner.isbn_field_name] for record in records]))
                        self.assertEqual(isbns, unmatched_isbns_expected)

                    if partner.type_id.startswith("jstor"):
                        # Run eISBN task - JSTOR is the only partner that uses eISBN
                        ti = env.run_task(task_id_qa_eisbn_fmt.format(data_partner_table_name=partner.bq_table_name))
                        self.assertEqual(ti.state, State.SUCCESS)
                        # Check eISBNs are valid
                        records = bq_run_query(data_qa_sql[f"{partner.type_id}_invalid_eisbn"])
                        eisbns = set(sorted([record["eISBN"] for record in records], key=lambda x: x or "0"))
                        self.assertEqual(eisbns, invalid_eisbns_expected)

                ##########################
                ### Export OAEBU Tables ##
                ##########################

                export_tables = [
                    ("book_product_list", 4),
                    ("book_product_metrics", 5),
                    ("book_product_metrics_country", 22),
                    ("book_product_metrics_institution", 1),
                    ("institution_list", 1),
                    ("book_product_metrics_city", 26),
                    ("book_product_metrics_events", 3),
                    ("book_product_publisher_metrics", 4),
                    ("book_product_subject_bic_metrics", 0),
                    ("book_product_subject_bisac_metrics", 0),
                    ("book_product_subject_thema_metrics", 0),
                    ("book_product_year_metrics", 5),
                    ("book_product_subject_year_metrics", 0),
                    ("book_product_author_metrics", 3),
                ]

                export_prefix = self.gcp_project_id.replace("-", "_")
                for table, exp_rows in export_tables:
                    ti = env.run_task(f"export_oaebu_table_{table}")
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assertEqual(expected_state, ti.state, msg=f"table: {table}")
                    # Check that the data_export tables tables exist and have the correct number of rows
                    table_id = bq_sharded_table_id(
                        self.gcp_project_id, oaebu_export_dataset_id, f"{export_prefix}_{table}", release_date
                    )
                    self.assert_table_integrity(table_id, expected_rows=exp_rows)

                # Book product list content assertion
                table_id = bq_sharded_table_id(
                    self.gcp_project_id, oaebu_export_dataset_id, f"{export_prefix}_book_product_list", release_date
                )
                fixture_table = load_and_parse_json(
                    os.path.join(self.fixtures_folder, "e2e_outputs", "book_product_list.json"),
                    date_fields=["published_date"],
                )
                self.assert_table_content(table_id, fixture_table, primary_key="product_id")

                # Export oaebu qa table
                ti = env.run_task(telescope.export_oaebu_qa_metrics.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                ################################
                ### Validate the joins worked ##
                ################################

                # JSTOR
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.jstor_country_matched{release_suffix}"
                records = bq_run_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}

                self.assertTrue(
                    oaebu_works["1111111111111"] == oaebu_works["2222222222222"]
                    and oaebu_works["113"] is None
                    and oaebu_works["111"] is None
                    and oaebu_works["211"] is None
                )

                self.assertTrue(
                    oaebu_wfam["1111111111111"] == oaebu_wfam["2222222222222"]
                    and oaebu_wfam["113"] is None
                    and oaebu_wfam["111"] is None
                    and oaebu_wfam["211"] is None
                )

                # IRUS OAPEN
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.irus_oapen_matched{release_suffix}"
                records = bq_run_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}

                self.assertTrue(
                    oaebu_works["1111111111111"] == oaebu_works["2222222222222"]
                    and oaebu_works["113"] is None
                    and oaebu_works["111"] is None
                    and oaebu_works["211"] is None
                )

                self.assertTrue(
                    oaebu_wfam["1111111111111"] == oaebu_wfam["2222222222222"]
                    and oaebu_wfam["113"] is None
                    and oaebu_wfam["111"] is None
                    and oaebu_wfam["211"] is None
                )

                #################################
                ### Create and validate views ###
                #################################

                ti = env.run_task(telescope.create_oaebu_latest_views.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Check data QA views are the same as the tables
                for k in data_qa_sql.keys():
                    table_data = bq_run_query(data_qa_sql[k])
                    view_data = bq_run_query(latest_data_qa_sql[k])
                    self.assertEqual(table_data, view_data)

                for export_table, exp_rows in export_tables:
                    export_view = bq_run_query(
                        f"SELECT * FROM {self.gcp_project_id}.{oaebu_latest_export_dataset_id}.{self.gcp_project_id.replace('-', '_')}_{export_table}"
                    )
                    self.assertEqual(expected_state, ti.state, msg=f"table: {table}")
                    # Check that the data_export tables (views) has the correct number of rows
                    self.assertEqual(len(export_view), exp_rows)

                ################################
                ### Add releases and Cleanup ###
                ################################

                # Add_dataset_release_task
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test cleanup
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(release.workflow_folder)

    def test_telescope(self):
        """Test that ONIX Workflow runs when Google Analytics is not included"""

        self.run_telescope_tests(include_google_analytics3=False)

    def test_telescope_with_google_analytics(self):
        """Test that ONIX Workflow runs when Google Analytics is included"""

        self.run_telescope_tests(include_google_analytics3=True)

    @patch("oaebu_workflows.onix_workflow.onix_workflow.bq_run_query")
    def test_get_onix_records(self, mock_bq_query):
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        records = get_onix_records("test_table_id")
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["ISBN13"], "111")
