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
#
#
# Author: Tuan Chien, Richard Hosking, Keegan Smith

import os
from datetime import timedelta
from typing import List, Optional, Tuple, Union
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pendulum
from google.cloud.bigquery import SourceFormat, Client
from ratelimit import limits, sleep_and_retry
from tenacity import wait_exponential_jitter

from oaebu_workflows.airflow_pools import CrossrefEventsPool
from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.config import sql_folder
from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from oaebu_workflows.onix_workflow.onix_work_aggregation import BookWorkAggregator, BookWorkFamilyAggregator
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import save_jsonl_gz
from observatory.platform.utils.url_utils import get_user_agent, retry_get_url
from observatory.platform.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    bq_load_table,
    bq_table_id,
    bq_sharded_table_id,
    bq_create_dataset,
    bq_create_table_from_query,
    bq_run_query,
    bq_select_table_shard_dates,
    bq_create_view,
    bq_find_schema,
)
from observatory.platform.workflows.workflow import (
    SnapshotRelease,
    Workflow,
    make_snapshot_date,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


CROSSREF_EVENT_URL_TEMPLATE = (
    "https://api.eventdata.crossref.org/v1/events?mailto={mailto}"
    "&from-collected-date={start_date}&until-collected-date={end_date}&rows=1000"
    "&obj-id={doi}"
)


class OnixWorkflowRelease(SnapshotRelease):
    """
    Release information for OnixWorkflow.
    """

    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        onix_snapshot_date: pendulum.DateTime,
        crossref_master_snapshot_date: pendulum.DateTime,
    ):
        """
        Construct the OnixWorkflow Release
        :param dag_id: DAG ID.
        :param release_date: The date of the partition/release
        :param onix_snapshot_date: The ONIX snapshot/release date.
        :param crossref_master_snapshot_date: The release date/suffix of the crossref master table

        """
        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.onix_snapshot_date = onix_snapshot_date
        self.crossref_master_snapshot_date = crossref_master_snapshot_date

        # Files
        self.workslookup_path = os.path.join(self.transform_folder, f"worksid.jsonl.gz")
        self.workslookup_errors_path = os.path.join(self.transform_folder, f"worksid_errors.jsonl.gz")
        self.worksfamilylookup_path = os.path.join(self.transform_folder, f"workfamilyid.jsonl.gz")
        self.crossref_metadata_path = os.path.join(self.transform_folder, f"crossref_metadata.jsonl.gz")
        self.crossref_events_path = os.path.join(self.transform_folder, f"crossref_events.jsonl.gz")


class OnixWorkflow(Workflow):
    """This workflow telescope:
    1. [Not implemented] Creates an ISBN13-> internal identifier lookup table.
    2. Creates an ISBN13 -> WorkID lookup table.
      a. Aggregates Product records into Work clusters.
      b. Writes the lookup table to BigQuery.
      c. Writes an error table to BigQuery.
    3. Create an ISBN13 -> Work Family ID lookup table.  Clusters editions together.
      a. Aggregate Works into Work Families.
      b. Writes the lookup table to BigQuery.
    4. Create OAEBU intermediate tables.
      a. For each data partner, create new tables in oaebu_intermediate dataset where existing tables are augmented
         with work_id and work_family_id columns.
    5. Create OAEBU QA tables for looking at metrics and problems arising from the data sets (and for eventual automatic reporting).
    """

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        metadata_partner: Union[str, OaebuPartner],
        # Bigquery parameters
        bq_master_crossref_project_id: str = "academic-observatory",
        bq_master_crossref_dataset_id: str = "crossref_metadata",
        bq_oaebu_crossref_dataset_id: str = "crossref",
        bq_master_crossref_metadata_table_name: str = "crossref_metadata",
        bq_oaebu_crossref_metadata_table_name: str = "crossref_metadata",
        bq_crossref_events_table_name: str = "crossref_events",
        bq_country_project_id: str = "oaebu-public-data",
        bq_country_dataset_id: str = "oaebu_reference",
        bq_subject_project_id: str = "oaebu-public-data",
        bq_subject_dataset_id: str = "oaebu_reference",
        bq_book_table_name: str = "book",
        bq_book_product_table_name: str = "book_product",
        bq_oaebu_data_qa_dataset: str = "oaebu_data_qa",
        bq_oaebu_latest_data_qa_dataset: str = "oaebu_data_qa_latset",
        bq_onix_workflow_dataset: str = "onix_workflow",
        bq_oaebu_intermediate_dataset: str = "oaebu_intermediate",
        bq_oaebu_dataset: str = "oaebu",
        bq_oaebu_export_dataset: str = "data_export",
        bq_oaebu_latest_export_dataset: str = "data_export_latest",
        bq_worksid_table_name: str = "onix_workid_isbn",
        bq_worksid_error_table_name: str = "onix_workid_isbn_errors",
        bq_workfamilyid_table_name: str = "onix_workfamilyid_isbn",
        bq_dataset_description: str = "ONIX workflow tables",
        oaebu_intermediate_match_suffix: str = "_matched",
        # Run parameters
        data_partners: List[Union[str, OaebuPartner]] = None,
        ga3_views_field="page_views",
        schema_folder: str = default_schema_folder(workflow_module="onix_workflow"),
        mailto: str = "agent@observatory.academy",
        crossref_start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        api_dataset_id: str = "onix_workflow",
        max_threads: int = 2 * os.cpu_count() - 1,
        # Ariflow parameters
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        sensor_dag_ids: List[str] = None,
        catchup: Optional[bool] = False,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2022, 8, 1),
        schedule: Optional[str] = "@weekly",
    ):
        """
        Initialises the workflow object.

        :param dag_id: DAG ID.
        :param cloud_workspace: The CloudWorkspace object for this DAG

        :param bq_master_crossref_project_id: GCP project ID of crossref master data
        :param bq_master_crossref_dataset_id: GCP dataset ID of crossref master data
        :param bq_oaebu_crossref_dataset_id: GCP dataset ID of crossref OAeBU data
        :param bq_master_crossref_metadata_table_name: The name of the master crossref metadata table
        :param bq_oaebu_crossref_metadata_table_name: The name of the OAeBU crossref metadata table
        :param bq_crossref_events_table_name: The name of the crossref events table
        :param bq_country_project_id: GCP project ID of the country table
        :param bq_country_dataset_id: GCP dataset containing the country table
        :param bq_subject_project_id: GCP project ID of the subject tables
        :param bq_subject_dataset_id: GCP dataset ID of the subject tables
        :param bq_book_table_name: The name of the book table
        :param bq_book_product_table_name: The name of the book product table
        :param bq_oaebu_data_qa_dataset: OAEBU Data QA dataset.
        :param bq_oaebu_latest_data_qa_dataset: OAEBU Data QA dataset with the latest data views
        :param bq_onix_workflow_dataset: Onix workflow dataset.
        :param bq_oaebu_intermediate_dataset: OAEBU intermediate dataset.
        :param bq_oaebu_dataset: OAEBU dataset.
        :param bq_oaebu_export_dataset: OAEBU data export dataset.
        :param bq_oaebu_latest_export_dataset: OAEBU data export dataset with the latest data views
        :param bq_worksid_table_name: table ID of the worksid table
        :param bq_worksid_error_table_name: table ID of the worksid error table
        :param bq_workfamilyid_table_name: table ID of the workfamilyid table
        :param bq_dataset_description: Description to give to the workflow tables
        :param oaebu_intermediate_match_suffix: Suffix to append to intermediate tables

        :param data_partners: OAEBU data sources.
        :param ga3_views_field: The name of the GA3 views field - should be either 'page_views' or 'unique_views'
        :param schema_folder: the SQL schema path.
        :param mailto: email address used to identify the user when sending requests to an API.
        :param crossref_start_date: The starting date of crossref's API calls
        :param api_dataset_id: The ID to store the dataset release in the API
        :param max_threads: The maximum number of threads to use for parallel tasks.

        :param observatory_api_conn_id: The connection ID for the observatory API
        :param sensor_dag_ids: Dag IDs for dependent tasks
        :param catchup: Whether to catch up missed DAG runs.
        :param start_date: Start date of the DAG.
        :param schedule: Scheduled interval for running the DAG.
        """

        if not sensor_dag_ids:
            sensor_dag_ids = []

        if data_partners is None:
            data_partners = list()

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
        # Bigquery projects, datasets and tables
        self.bq_master_crossref_project_id = bq_master_crossref_project_id
        self.bq_master_crossref_dataset_id = bq_master_crossref_dataset_id
        self.bq_oaebu_crossref_dataset_id = bq_oaebu_crossref_dataset_id
        self.bq_master_crossref_metadata_table_name = bq_master_crossref_metadata_table_name
        self.bq_oaebu_crossref_metadata_table_name = bq_oaebu_crossref_metadata_table_name
        self.bq_crossref_events_table_name = bq_crossref_events_table_name
        self.bq_country_project_id = bq_country_project_id
        self.bq_country_dataset_id = bq_country_dataset_id
        self.bq_subject_project_id = bq_subject_project_id
        self.bq_subject_dataset_id = bq_subject_dataset_id
        self.bq_book_table_name = bq_book_table_name
        self.bq_book_product_table_name = bq_book_product_table_name
        self.bq_oaebu_data_qa_dataset = bq_oaebu_data_qa_dataset
        self.bq_oaebu_latest_data_qa_dataset = bq_oaebu_latest_data_qa_dataset
        self.bq_onix_workflow_dataset = bq_onix_workflow_dataset
        self.bq_oaebu_intermediate_dataset = bq_oaebu_intermediate_dataset
        self.bq_oaebu_dataset = bq_oaebu_dataset
        self.bq_oaebu_export_dataset = bq_oaebu_export_dataset
        self.bq_oaebu_latest_export_dataset = bq_oaebu_latest_export_dataset
        self.bq_worksid_table_name = bq_worksid_table_name
        self.bq_worksid_error_table_name = bq_worksid_error_table_name
        self.bq_workfamilyid_table_name = bq_workfamilyid_table_name
        self.bq_dataset_description = bq_dataset_description
        self.oaebu_intermediate_match_suffix = oaebu_intermediate_match_suffix
        # Run parameters
        self.data_partners = [partner_from_str(p) for p in data_partners]
        self.ga3_views_field = ga3_views_field
        self.schema_folder = schema_folder
        self.mailto = mailto
        self.crossref_start_date = crossref_start_date
        self.api_dataset_id = api_dataset_id
        self.max_threads = max_threads
        # Airflow Parameters
        self.observatory_api_conn_id = observatory_api_conn_id
        self.sensor_dag_ids = sensor_dag_ids
        self.catchup = catchup
        self.start_date = start_date
        self.schedule = schedule

        # Initialise Telesecope base class
        super().__init__(
            dag_id=self.dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=catchup,
            airflow_conns=[observatory_api_conn_id],
            tags=["oaebu"],
        )

        check_workflow_inputs(self)

        # Create pools for crossref API calls (if they don't exist)
        # Pools are necessary to throttle the maxiumum number of requests we can make per second and avoid 429 errors
        crossref_events_pool = CrossrefEventsPool(pool_slots=15)
        crossref_events_pool.create_pool()

        # Wait for external workflows to finish
        with self.parallel_tasks():
            for ext_dag_id in self.sensor_dag_ids:
                sensor = DagRunSensor(
                    task_id=f"{ext_dag_id}_sensor",
                    external_dag_id=ext_dag_id,
                    mode="reschedule",
                    duration=timedelta(days=7),  # Look back up to 7 days from execution date
                    poke_interval=int(timedelta(hours=1).total_seconds()),  # Check at this interval if dag run is ready
                    timeout=int(timedelta(days=2).total_seconds()),  # Sensor will fail after 2 days of waiting
                )
                self.add_operator(sensor)

        # Aggregate Works
        self.add_task(self.aggregate_works)
        self.add_task(self.upload_aggregation_tables)
        self.add_task(self.bq_load_aggregations)

        # Create crossref metadata and event tables
        self.add_task(self.create_oaebu_crossref_metadata_table)
        self.add_task(
            self.create_oaebu_crossref_events_table,
            pool=crossref_events_pool.pool_name,
            pool_slots=min(self.max_threads, crossref_events_pool.pool_slots),
        )

        # Create book table
        self.add_task(self.create_oaebu_book_table)

        # Create OAEBU Intermediate tables (not for onix partner types)
        with self.parallel_tasks():
            for data_partner in self.data_partners:
                task_id = f"create_oaebu_intermediate_table_{data_partner.bq_table_name}"

                self.add_task(
                    self.create_oaebu_intermediate_table,
                    op_kwargs=dict(
                        orig_project_id=self.cloud_workspace.project_id,
                        orig_dataset=data_partner.bq_dataset_id,
                        orig_table=data_partner.bq_table_name,
                        orig_isbn=data_partner.isbn_field_name,
                        sharded=data_partner.sharded,
                    ),
                    task_id=task_id,
                )

        # Create OAEBU tables
        self.add_task(self.create_oaebu_book_product_table)

        # Create QA metrics tables
        self.create_oaebu_data_qa_tasks()

        # Create OAEBU Elastic Export tables
        self.create_oaebu_export_tasks()

        # Create the (non-sharded) views of the sharded tables
        self.add_task(self.create_oaebu_latest_views)

        # Final tasks
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> OnixWorkflowRelease:
        """Creates a release object.
        :param kwargs: From Airflow. Contains the execution_date.
        :return: an OnixWorkflowRelease object.
        """

        # Make snapshot date
        snapshot_date = make_snapshot_date(**kwargs)

        # Get ONIX release date
        onix_table_id = bq_table_id(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.metadata_partner.bq_dataset_id,
            table_id=self.metadata_partner.bq_table_name,
        )
        onix_snapshot_dates = bq_select_table_shard_dates(table_id=onix_table_id, end_date=snapshot_date)
        assert len(onix_snapshot_dates), "OnixWorkflow.make_release: no ONIX releases found"
        onix_snapshot_date = onix_snapshot_dates[0]  # Get most recent snapshot

        # Get Crossref Metadata release date
        crossref_table_id = bq_table_id(
            project_id=self.bq_master_crossref_project_id,
            dataset_id=self.bq_master_crossref_dataset_id,
            table_id=self.bq_master_crossref_metadata_table_name,
        )
        crossref_metadata_snapshot_dates = bq_select_table_shard_dates(
            table_id=crossref_table_id, end_date=snapshot_date
        )
        assert len(crossref_metadata_snapshot_dates), "OnixWorkflow.make_release: no Crossref Metadata releases found"
        crossref_master_snapshot_date = crossref_metadata_snapshot_dates[0]  # Get most recent snapshot

        # Make the release object
        return OnixWorkflowRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            snapshot_date=snapshot_date,
            onix_snapshot_date=onix_snapshot_date,
            crossref_master_snapshot_date=crossref_master_snapshot_date,
        )

    def aggregate_works(self, release: OnixWorkflowRelease, **kwargs):
        """Fetches the ONIX product records from our ONIX database, aggregates them into works, workfamilies,
        and outputs it into jsonl files."""

        # Fetch ONIX data
        sharded_onix_table = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )
        products = get_onix_records(sharded_onix_table)

        # Aggregate into works
        agg = BookWorkAggregator(products)
        works = agg.aggregate()
        lookup_table = agg.get_works_lookup_table()
        save_jsonl_gz(release.workslookup_path, lookup_table)

        # Save errors from aggregation
        error_table = [{"Error": error} for error in agg.errors]
        save_jsonl_gz(release.workslookup_errors_path, error_table)

        # Aggregate work families
        agg = BookWorkFamilyAggregator(works)
        agg.aggregate()
        lookup_table = agg.get_works_family_lookup_table()
        save_jsonl_gz(release.worksfamilylookup_path, lookup_table)

    def upload_aggregation_tables(self, release: OnixWorkflowRelease, **kwargs):
        """Upload the aggregation tables and error tables to a GCP bucket in preparation for BQ loading."""
        files = [release.workslookup_path, release.workslookup_errors_path, release.worksfamilylookup_path]
        gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=files)

    def bq_load_aggregations(self, release: OnixWorkflowRelease, **kwargs):
        """Loads the 'WorkID lookup', 'WorkID lookup table errors' and 'WorkFamilyID lookup' tables into BigQuery."""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_onix_workflow_dataset,
            location=self.cloud_workspace.data_location,
            description="Onix Workflow Aggregations",
        )

        aggregation_paths = [release.workslookup_path, release.workslookup_errors_path, release.worksfamilylookup_path]
        aggregation_tables = [
            self.bq_worksid_table_name,
            self.bq_worksid_error_table_name,
            self.bq_workfamilyid_table_name,
        ]
        for path, table_name in zip(aggregation_paths, aggregation_tables):
            uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(path))
            table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id, self.bq_onix_workflow_dataset, table_name, release.snapshot_date
            )
            state = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=bq_find_schema(path=self.schema_folder, table_name=table_name),
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            set_task_state(state, kwargs["ti"].task_id, release=release)

    def create_oaebu_crossref_metadata_table(self, release: OnixWorkflowRelease, **kwargs):
        """Creates the crossref metadata table by querying the AO master table and matching on this publisher's ISBNs

        :param release: The onix workflow release object
        """
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_crossref_dataset_id,
            location=self.cloud_workspace.data_location,
            description="Data from Crossref sources",
        )

        onix_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )
        master_crossref_metadata_table_id = bq_sharded_table_id(
            self.bq_master_crossref_project_id,
            self.bq_master_crossref_dataset_id,
            self.bq_master_crossref_metadata_table_name,
            release.crossref_master_snapshot_date,
        )
        sql = render_template(
            os.path.join(sql_folder(workflow_module="onix_workflow"), "crossref_metadata_filter_isbn.sql.jinja2"),
            onix_table_id=onix_table_id,
            crossref_metadata_table_id=master_crossref_metadata_table_id,
        )
        print("Creating crossref metadata table from master table")
        schema_file_path = bq_find_schema(
            path=self.schema_folder, table_name=self.bq_oaebu_crossref_metadata_table_name
        )
        oaebu_crossref_metadata_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_oaebu_crossref_metadata_table_name,
            release.snapshot_date,
        )
        state = bq_create_table_from_query(
            sql=sql, table_id=oaebu_crossref_metadata_table_id, schema_file_path=schema_file_path
        )
        set_task_state(state, kwargs["ti"].task_id, release=release)

    def create_oaebu_crossref_events_table(self, release: OnixWorkflowRelease, **kwargs):
        """Download, transform, upload and create a table for crossref events"""

        # Get the unique dois from the metadata table
        metadata_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_oaebu_crossref_metadata_table_name,
            release.snapshot_date,
        )
        dois = dois_from_table(metadata_table_id, doi_column_name="DOI", distinct=True)

        # Download and transform all events
        start_date = self.crossref_start_date
        end_date = release.snapshot_date.subtract(days=1).date()
        events = download_crossref_events(dois, start_date, end_date, self.mailto, max_threads=self.max_threads)
        events = transform_crossref_events(events, max_threads=self.max_threads)

        # Zip and upload to google cloud
        save_jsonl_gz(release.crossref_events_path, events)
        gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.crossref_events_path])
        uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.crossref_events_path))
        table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_crossref_events_table_name,
            release.snapshot_date,
        )
        state = bq_load_table(
            uri=uri,
            table_id=table_id,
            schema_file_path=bq_find_schema(path=self.schema_folder, table_name=self.bq_crossref_events_table_name),
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        set_task_state(state, kwargs["ti"].task_id, release=release)

    def create_oaebu_book_table(self, release: OnixWorkflowRelease, **kwargs):
        """Create the oaebu book table using the crossref event and metadata tables"""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Tables",
        )
        book_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_dataset, self.bq_book_table_name, release.snapshot_date
        )
        crossref_metadata_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_oaebu_crossref_metadata_table_name,
            release.snapshot_date,
        )
        crossref_events_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_crossref_events_table_name,
            release.snapshot_date,
        )
        sql = render_template(
            os.path.join(sql_folder(workflow_module="onix_workflow"), "create_book.sql.jinja2"),
            crossref_events_table_id=crossref_events_table_id,
            crossref_metadata_table_id=crossref_metadata_table_id,
        )
        status = bq_create_table_from_query(
            sql=sql,
            table_id=book_table_id,
            schema_file_path=bq_find_schema(path=self.schema_folder, table_name=self.bq_book_table_name),
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def create_oaebu_intermediate_table(
        self,
        release: OnixWorkflowRelease,
        *,
        orig_project_id: str,
        orig_dataset: str,
        orig_table: str,
        orig_isbn: str,
        sharded: bool,
        **kwargs,
    ):
        """Create an intermediate oaebu table.  They are of the form datasource_matched<date>
        :param release: Onix workflow release information.
        :param orig_project_id: Project ID for the partner data.
        :param orig_dataset: Dataset ID for the partner data.
        :param orig_table: Table ID for the partner data.
        :param orig_isbn: Name of the ISBN field in the partner data table.
        :param sharded: Whether the data partner table is sharded
        """
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_intermediate_dataset,
            location=self.cloud_workspace.data_location,
            description="Intermediate OAEBU Tables",
        )
        orig_table_id = (
            bq_sharded_table_id(orig_project_id, orig_dataset, orig_table, release.snapshot_date)
            if sharded
            else bq_table_id(orig_project_id, orig_dataset, orig_table)
        )
        output_table_name = f"{orig_table}{self.oaebu_intermediate_match_suffix}"
        template_path = os.path.join(
            sql_folder(workflow_module="onix_workflow"), "assign_workid_workfamilyid.sql.jinja2"
        )
        output_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_intermediate_dataset,
            output_table_name,
            release.snapshot_date,
        )
        wid_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_worksid_table_name,
            release.snapshot_date,
        )
        wfam_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_workfamilyid_table_name,
            release.snapshot_date,
        )

        # Make the table from SQL query
        sql = render_template(
            template_path,
            orig_table_id=orig_table_id,
            orig_isbn=orig_isbn,
            wid_table_id=wid_table_id,
            wfam_table_id=wfam_table_id,
        )
        status = bq_create_table_from_query(sql=sql, table_id=output_table_id)
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def create_oaebu_book_product_table(
        self,
        release: OnixWorkflowRelease,
        **kwargs,
    ):
        """Create the Book Product Table"""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Tables",
        )
        data_partner_datasets = {data.type_id: data.bq_dataset_id for data in self.data_partners}
        google_analytics_dataset = data_partner_datasets.get("google_analytics3")
        google_books_dataset = data_partner_datasets.get("google_books_traffic")
        jstor_dataset = data_partner_datasets.get("jstor_country")
        irus_oapen_dataset = data_partner_datasets.get("irus_oapen")
        irus_fulcrum_dataset = data_partner_datasets.get("irus_fulcrum")
        ucl_discovery_dataset = data_partner_datasets.get("ucl_discovery")
        internet_archive_dataset = data_partner_datasets.get("internet_archive")
        worldreader_dataset = data_partner_datasets.get("worldreader")

        # Create matched tables for supplied data partners
        google_analytics3_table_id = "empty_google_analytics3"
        google_books_sales_table_id = "empty_google_books_sales"
        google_books_traffic_table_id = "empty_google_books_traffic"
        jstor_country_table_id = "empty_jstor_country"
        jstor_institution_table_id = "empty_jstor_institution"
        irus_oapen_table_id = "empty_irus_oapen"
        irus_fulcrum_table_id = "empty_irus_fulcrum"
        ucl_discovery_table_id = "empty_ucl_discovery"
        internet_archive_table_id = "empty_internet_archive"
        worldreader_table_id = "empty_worldreader"
        if google_analytics_dataset:
            google_analytics3_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "google_analytics3_matched",
                release.snapshot_date,
            )
        if google_books_dataset:
            google_books_sales_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "google_books_sales_matched",
                release.snapshot_date,
            )
            google_books_traffic_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "google_books_traffic_matched",
                release.snapshot_date,
            )
        if jstor_dataset:
            jstor_country_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "jstor_country_matched",
                release.snapshot_date,
            )
            jstor_institution_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "jstor_institution_matched",
                release.snapshot_date,
            )
        if irus_oapen_dataset:
            irus_oapen_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "irus_oapen_matched",
                release.snapshot_date,
            )
        if ucl_discovery_dataset:
            ucl_discovery_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "ucl_discovery_matched",
                release.snapshot_date,
            )
        if irus_fulcrum_dataset:
            irus_fulcrum_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "irus_fulcrum_matched",
                release.snapshot_date,
            )
        if internet_archive_dataset:
            internet_archive_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "internet_archive_matched",
                release.snapshot_date,
            )
        if worldreader_dataset:
            worldreader_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                "worldreader_matched",
                release.snapshot_date,
            )
        workid_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_worksid_table_name,
            release.snapshot_date,
        )
        workfamilyid_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_workfamilyid_table_name,
            release.snapshot_date,
        )
        book_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_dataset, self.bq_book_table_name, release.snapshot_date
        )
        onix_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )
        country_table_id = bq_table_id(self.bq_country_project_id, self.bq_country_dataset_id, "country")
        template_path = os.path.join(sql_folder(workflow_module="onix_workflow"), "create_book_products.sql.jinja2")
        sql = render_template(
            template_path,
            onix_table_id=onix_table_id,
            google_analytics3_table_id=google_analytics3_table_id,
            google_books_sales_table_id=google_books_sales_table_id,
            google_books_traffic_table_id=google_books_traffic_table_id,
            jstor_country_table_id=jstor_country_table_id,
            jstor_institution_table_id=jstor_institution_table_id,
            irus_oapen_table_id=irus_oapen_table_id,
            irus_fulcrum_table_id=irus_fulcrum_table_id,
            ucl_discovery_table_id=ucl_discovery_table_id,
            internet_archive_table_id=internet_archive_table_id,
            worldreader_table_id=worldreader_table_id,
            book_table_id=book_table_id,
            country_table_id=country_table_id,
            workid_table_id=workid_table_id,
            workfamilyid_table_id=workfamilyid_table_id,
            ga3_views_field=self.ga3_views_field,
            onix_workflow=True,
        )
        schema_file_path = bq_find_schema(path=self.schema_folder, table_name=self.bq_book_product_table_name)
        table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_dataset,
            self.bq_book_product_table_name,
            release.snapshot_date,
        )
        status = bq_create_table_from_query(sql=sql, table_id=table_id, schema_file_path=schema_file_path)
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def export_oaebu_table(
        self,
        release: OnixWorkflowRelease,
        **kwargs,
    ):
        """Create an intermediate oaebu table. They are of the form datasource_matched<date>"""
        output_table: str = kwargs["output_table"]
        query_template: str = kwargs["query_template"]
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_export_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Tables for Dashboarding",
        )
        output_table_name = f"{self.cloud_workspace.project_id.replace('-', '_')}_{output_table}"
        output_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_export_dataset, output_table_name, release.snapshot_date
        )
        template_path = os.path.join(sql_folder(workflow_module="onix_workflow"), query_template)
        schema_file_path = bq_find_schema(
            path=self.schema_folder,
            table_name=output_table,
            prefix="oaebu_publisher_",
        )
        book_product_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_dataset,
            self.bq_book_product_table_name,
            release.snapshot_date,
        )
        country_table_id = bq_table_id(self.bq_country_project_id, self.bq_country_dataset_id, "country")
        bic_table_id = bq_table_id(self.bq_subject_project_id, self.bq_subject_dataset_id, "bic_lookup")
        bisac_table_id = bq_table_id(self.bq_subject_project_id, self.bq_subject_dataset_id, "bisac_lookup")
        thema_table_id = bq_table_id(self.bq_subject_project_id, self.bq_subject_dataset_id, "thema_lookup")
        sql = render_template(
            template_path,
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_dataset,
            release=release.snapshot_date,
            book_product_table_id=book_product_table_id,
            country_table_id=country_table_id,
            bic_table_id=bic_table_id,
            bisac_table_id=bisac_table_id,
            thema_table_id=thema_table_id,
        )
        status = bq_create_table_from_query(sql=sql, table_id=output_table_id, schema_file_path=schema_file_path)
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def export_oaebu_qa_metrics(
        self,
        release: OnixWorkflowRelease,
        **kwargs,
    ):
        """Create the unmatched metrics table"""
        data_partner_isbns = {data.type_id: data.isbn_field_name for data in self.data_partners}
        data_partner_tables = {data.type_id: data.bq_table_name for data in self.data_partners}
        google_analytics3_table = data_partner_tables.get("google_analytics3")
        google_books_table = data_partner_tables.get("google_books_traffic")
        jstor_table = data_partner_tables.get("jstor_country")
        irus_oapen_table = data_partner_tables.get("irus_oapen")
        ucl_table = data_partner_tables.get("ucl_discovery")
        fulcrum_table = data_partner_tables.get("irus_fulcrum")

        google_analytics3_unmatched_table_id = (
            bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_data_qa_dataset,
                f"{google_analytics3_table}_unmatched_{data_partner_isbns['google_analytics3']}",
                release.snapshot_date,
            )
            if google_analytics3_table
            else None
        )
        google_books_unmatched_table_id = (
            bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_data_qa_dataset,
                f"{google_books_table}_unmatched_{data_partner_isbns['google_books_traffic']}",
                release.snapshot_date,
            )
            if google_books_table
            else None
        )
        jstor_unmatched_table_id = (
            bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_data_qa_dataset,
                f"{jstor_table}_unmatched_{data_partner_isbns['jstor_country']}",
                release.snapshot_date,
            )
            if jstor_table
            else None
        )
        irus_oapen_unmatched_table_id = (
            bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_data_qa_dataset,
                f"{irus_oapen_table}_unmatched_{data_partner_isbns['irus_oapen']}",
                release.snapshot_date,
            )
            if irus_oapen_table
            else None
        )
        ucl_discovery_unmatched_table_id = (
            bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_data_qa_dataset,
                f"{ucl_table}_unmatched_{data_partner_isbns['ucl_discovery']}",
                release.snapshot_date,
            )
            if ucl_table
            else None
        )
        fulcrum_unmatched_table_id = (
            bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_data_qa_dataset,
                f"{fulcrum_table}_unmatched_{data_partner_isbns['irus_fulcrum']}",
                release.snapshot_date,
            )
            if fulcrum_table
            else None
        )
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_export_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Tables for Dashboarding",
        )
        table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_export_dataset,
            f"{self.cloud_workspace.project_id.replace('-', '_')}_unmatched_book_metrics",
            release.snapshot_date,
        )
        template_path = os.path.join(sql_folder(workflow_module="onix_workflow"), "export_unmatched_metrics.sql.jinja2")
        sql = render_template(
            template_path,
            google_analytics3_unmatched_table_id=google_analytics3_unmatched_table_id,
            google_books_unmatched_table_id=google_books_unmatched_table_id,
            jstor_unmatched_table_id=jstor_unmatched_table_id,
            irus_oapen_unmatched_table_id=irus_oapen_unmatched_table_id,
            ucl_discovery_unmatched_table_id=ucl_discovery_unmatched_table_id,
            fulcrum_unmatched_table_id=fulcrum_unmatched_table_id,
            google_analytics3_isbn=data_partner_isbns.get("google_analytics3"),
            google_books_isbn=data_partner_isbns.get("google_books_traffic"),
            jstor_isbn=data_partner_isbns.get("jstor_country"),
            irus_oapen_isbn=data_partner_isbns.get("irus_oapen"),
            ucl_discovery_isbn=data_partner_isbns.get("ucl_discovery"),
            fulcrum_isbn=data_partner_isbns.get("irus_fulcrum"),
        )
        status = bq_create_table_from_query(sql=sql, table_id=table_id)
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def create_oaebu_export_tasks(self):
        """Create tasks for exporting final metrics from our OAEBU data.  It will create output tables in the oaebu_elastic dataset."""
        export_tables = [
            {"output_table": "book_product_list", "query_template": "export_book_list.sql.jinja2"},
            {
                "output_table": "book_product_metrics",
                "query_template": "export_book_metrics.sql.jinja2",
            },
            {
                "output_table": "book_product_metrics_country",
                "query_template": "export_book_metrics_country.sql.jinja2",
            },
            {
                "output_table": "book_product_metrics_institution",
                "query_template": "export_book_metrics_institution.sql.jinja2",
            },
            {
                "output_table": "institution_list",
                "query_template": "export_institution_list.sql.jinja2",
            },
            {
                "output_table": "book_product_metrics_city",
                "query_template": "export_book_metrics_city.sql.jinja2",
            },
            {
                "output_table": "book_product_metrics_events",
                "query_template": "export_book_metrics_event.sql.jinja2",
            },
            {
                "output_table": "book_product_publisher_metrics",
                "query_template": "export_book_publisher_metrics.sql.jinja2",
            },
            {
                "output_table": "book_product_subject_bic_metrics",
                "query_template": "export_book_subject_bic_metrics.sql.jinja2",
            },
            {
                "output_table": "book_product_subject_bisac_metrics",
                "query_template": "export_book_subject_bisac_metrics.sql.jinja2",
            },
            {
                "output_table": "book_product_subject_thema_metrics",
                "query_template": "export_book_subject_thema_metrics.sql.jinja2",
            },
            {
                "output_table": "book_product_year_metrics",
                "query_template": "export_book_year_metrics.sql.jinja2",
            },
            {
                "output_table": "book_product_subject_year_metrics",
                "query_template": "export_book_subject_year_metrics.sql.jinja2",
            },
            {
                "output_table": "book_product_author_metrics",
                "query_template": "export_book_author_metrics.sql.jinja2",
            },
        ]

        # Create each export table in BiqQuery
        with self.parallel_tasks():
            for export_table in export_tables:
                task_id = f"export_oaebu_table_{export_table['output_table']}"
                self.add_task(
                    self.export_oaebu_table,
                    op_kwargs=dict(
                        output_table=export_table["output_table"], query_template=export_table["query_template"]
                    ),
                    task_id=task_id,
                )

        # Export QA Metrics
        self.add_task(self.export_oaebu_qa_metrics)

    def create_oaebu_data_qa_tasks(self):
        """Create tasks for outputing QA metrics from our OAEBU data.
        It will create output tables in the oaebu_data_qa dataset.
        """
        with self.parallel_tasks():
            self.add_task(self.create_oaebu_data_qa_isbn_onix)
            self.add_task(self.create_oaebu_data_qa_onix_aggregate)
            for data_partner in self.data_partners:
                task_id_invalid_isbn = f"create_oaebu_data_qa_isbn_{data_partner.bq_table_name}"
                task_id_unmatched_isbn = f"create_oaebu_data_qa_intermediate_unmatched_{data_partner.bq_table_name}"
                self.add_task(
                    self.create_oaebu_data_qa_intermediate_unmatched_workid,
                    op_kwargs=dict(data_partner=data_partner),
                    task_id=task_id_unmatched_isbn,
                )
                # JSTOR has QA for EISBN and collates country + institution
                if data_partner.type_id.startswith("jstor"):
                    self.add_task(
                        self.create_oaebu_data_qa_isbn,
                        op_kwargs=dict(
                            data_partner=data_partner,
                            table_name="jstor_invalid_isbn",
                            isbn=data_partner.isbn_field_name,
                        ),
                        task_id=task_id_invalid_isbn,
                    )
                    self.add_task(
                        self.create_oaebu_data_qa_isbn,
                        op_kwargs=dict(
                            data_partner=data_partner,
                            table_name="jstor_invalid_eisbn",
                            isbn="eISBN",
                        ),
                        task_id=task_id_invalid_isbn.replace("isbn", "eisbn"),
                    )
                else:
                    self.add_task(
                        self.create_oaebu_data_qa_isbn,
                        op_kwargs=dict(
                            data_partner=data_partner,
                            table_name=f"{data_partner.type_id}_invalid_isbn",
                            isbn=data_partner.isbn_field_name,
                        ),
                        task_id=task_id_invalid_isbn,
                    )

    def create_oaebu_latest_views(self, release: OnixWorkflowRelease, **kwargs):
        """Create views of the latest data export tables in bigquery"""
        create_latest_views_from_dataset(
            project_id=self.cloud_workspace.project_id,
            from_dataset=self.bq_oaebu_data_qa_dataset,
            to_dataset=self.bq_oaebu_latest_data_qa_dataset,
            date_match=release.snapshot_date.strftime("%Y%m%d"),
            data_location=self.cloud_workspace.data_location,
        )
        create_latest_views_from_dataset(
            project_id=self.cloud_workspace.project_id,
            from_dataset=self.bq_oaebu_export_dataset,
            to_dataset=self.bq_oaebu_latest_export_dataset,
            date_match=release.snapshot_date.strftime("%Y%m%d"),
            data_location=self.cloud_workspace.data_location,
        )

    def create_oaebu_data_qa_onix_aggregate(self, release: OnixWorkflowRelease, **kwargs):
        """Create a bq table of some aggregate metrics for the ONIX data set."""
        onix_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )
        output_table_name = "onix_aggregate_metrics"
        table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_data_qa_dataset,
            output_table_name,
            release.snapshot_date,
        )
        template_path = os.path.join(sql_folder(workflow_module="onix_workflow"), "onix_aggregate_metrics.sql.jinja2")
        sql = render_template(template_path, table_id=onix_table_id, isbn="ISBN13")
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_data_qa_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Quality Analysis Tables",
        )
        schema_file_path = bq_find_schema(path=self.schema_folder, table_name=output_table_name)
        status = bq_create_table_from_query(
            sql=sql,
            table_id=table_id,
            schema_file_path=schema_file_path,
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def create_oaebu_data_qa_isbn_onix(self, release: OnixWorkflowRelease, **kwargs):
        """Create a BQ table of invalid ISBNs for the ONIX feed that can be fed back to publishers.
        No attempt is made to normalise the string so we catch as many string issues as we can.
        """
        orig_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )
        output_table_name = "onix_invalid_isbn"
        output_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_data_qa_dataset, output_table_name, release.snapshot_date
        )
        status = self.oaebu_data_qa_validate_isbn(
            orig_table_id=orig_table_id,
            output_table_id=output_table_id,
            isbn="ISBN13",
            schema_file_path=bq_find_schema(path=self.schema_folder, table_name=output_table_name),
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def oaebu_data_qa_validate_isbn(
        self,
        *,
        orig_table_id: str,
        output_table_id: str,
        isbn: str,
        schema_file_path: str = None,
    ) -> bool:
        """Create a BQ table of invalid ISBNs for the ONIX feed that can be fed back to publishers.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param orig_table_id: Fully qualified table ID of the source data
        :param output_table_id: Fully qualified table ID for the output data.
        :param isbn: Name of the isbn field in source table.
        :param schema_file_path: The path of the schema file to use for the BigQuery upload
        :return: The status of the table creation
        """
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_data_qa_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Quality Analysis Tables",
        )
        sql_template = os.path.join(sql_folder(workflow_module="onix_workflow"), "validate_isbn.sql.jinja2")
        sql = get_isbn_utils_sql_string() + render_template(sql_template, table_id=orig_table_id, isbn=isbn)
        status = bq_create_table_from_query(sql=sql, table_id=output_table_id, schema_file_path=schema_file_path)
        return status

    def create_oaebu_data_qa_isbn(
        self,
        release: OnixWorkflowRelease,
        data_partner: OaebuPartner,
        table_name: str,
        isbn: str,
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the Google Analytics feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param release: workflow release object.
        :param data_partner: OaebuPartner,
        :param table_name: The name of the table to create
        :param isbn: Name of the isbn field in source table.
        """

        if data_partner.sharded:
            orig_table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id,
                data_partner.bq_dataset_id,
                data_partner.bq_table_name,
                release.snapshot_date,
            )
        else:
            orig_table_id = bq_table_id(
                self.cloud_workspace.project_id,
                data_partner.bq_dataset_id,
                data_partner.bq_table_name,
            )

        output_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_data_qa_dataset,
            table_name,
            release.snapshot_date,
        )
        # Validate the ISBN field
        status = self.oaebu_data_qa_validate_isbn(
            orig_table_id=orig_table_id,
            output_table_id=output_table_id,
            isbn=isbn,
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def create_oaebu_data_qa_intermediate_unmatched_workid(
        self,
        release: OnixWorkflowRelease,
        data_partner: OaebuPartner,
        *args,
        **kwargs,
    ):
        """Create quality assurance metrics for the OAEBU intermediate tables.
        :param data_partner: The OaebuPartner to use
        """
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_data_qa_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Quality Analysis Tables",
        )
        template_path = os.path.join(
            sql_folder(workflow_module="onix_workflow"), "oaebu_intermediate_metrics.sql.jinja2"
        )
        intermediate_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_intermediate_dataset,
            f"{data_partner.bq_table_name}_matched",
            release.snapshot_date,
        )
        sql = render_template(
            template_path,
            table_id=intermediate_table_id,
            isbn=data_partner.isbn_field_name,
            title=data_partner.title_field_name,
        )
        output_table_name = f"{data_partner.bq_table_name}_unmatched_{data_partner.isbn_field_name}"
        output_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_data_qa_dataset, output_table_name, release.snapshot_date
        )
        status = bq_create_table_from_query(sql=sql, table_id=output_table_id)
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def add_new_dataset_releases(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: OnixWorkflowRelease, **kwargs):
        """Cleanup temporary files."""
        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


def dois_from_table(table_id: str, doi_column_name: str = "DOI", distinct: str = True) -> List[str]:
    """
    Queries a metadata table to retrieve the unique DOIs. Provided the DOIs are not in a nested structure.

    :param metadata_table_id: The fully qualified ID of the metadata table on GCP
    :param doi_field_name: The name of the DOI column
    :param distinct: Whether to retrieve only unique DOIs
    :return: All DOIs present in the metadata table
    """
    select_field = f"DISTINCT({doi_column_name})" if distinct else doi_column_name
    sql = f"SELECT {select_field} FROM `{table_id}`"
    query_results = bq_run_query(sql)
    dois = [r["DOI"] for r in query_results]
    return dois


def download_crossref_events(
    dois: List[str],
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    mailto: str,
    max_threads: int = 1,
) -> List[dict]:
    """
    Spawns multiple threads to download event data (DOI and publisher only) for each doi supplied.
    The url template was made with reference to the crossref event api:
    https://www.eventdata.crossref.org/guide/service/query-api/
    Note that the max_threads will cap at 15 because the events API will return a 429 if more than 15 requests are made
    per second. Each API request happens to take roughly 1 second. Having more threadsthan necessary slows down the
    download process as the retry script will wait a minimum of two seconds between each attempt.

    :param dois: The list of DOIs to download the events for
    :param start_date: The start date for events we're interested in
    :param end_date: The end date for events we're interested in
    :param mailto: The email to use as a reference for who is requesting the data
    :param max_threads: The maximum threads to spawn for the downloads.
    :return: All events for the input DOIs
    """
    event_url_template = CROSSREF_EVENT_URL_TEMPLATE
    url_start_date = start_date.strftime("%Y-%m-%d")
    url_end_date = end_date.strftime("%Y-%m-%d")
    max_threads = min(max_threads, 15)

    event_urls = [
        event_url_template.format(doi=doi, mailto=mailto, start_date=url_start_date, end_date=url_end_date)
        for doi in dois
    ]

    print(f"Beginning crossref event data download from {len(event_urls)} URLs with {max_threads} workers")
    print(
        f"Downloading DOI data using URL: {event_url_template.format(doi='***', mailto=mailto, start_date=url_start_date, end_date=url_end_date)}"
    )
    all_events = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i, url in enumerate(event_urls):
            futures.append(executor.submit(download_crossref_event_url, url, i=i))
        for future in as_completed(futures):
            all_events.extend(future.result())

    return all_events


def download_crossref_event_url(url: str, i: int = 0) -> List[dict]:
    """
    Downloads all crossref events from a url, iterating through pages if there is more than one

    :param url: The url send the request to
    :param i: Worker number
    :return: The events from this URL
    """
    events = []
    headers = {"User-Agent": get_user_agent(package_name="oaebu_workflows")}
    next_cursor, page_counts, total_events, page_events = download_crossref_page_events(url, headers)
    events.extend(page_events)
    total_counts = page_counts
    while next_cursor:
        tmp_url = url + f"&cursor={next_cursor}"
        next_cursor, page_counts, _, page_events = download_crossref_page_events(tmp_url, headers)
        total_counts += page_counts
        events.extend(page_events)
    print(f"{i + 1}: {url} successful")
    print(f"{i + 1}: Total no. events: {total_events}, downloaded " f"events: {total_counts}")
    return events


def download_crossref_page_events(url: str, headers: dict) -> Tuple[str, int, int, List[dict]]:
    """
    Download crossref events from a single page

    :param url: The url to send the request to
    :param headers: Headers to send with the request
    :return: The cursor, event counter, total number of events and the events for the URL
    """
    crossref_events_limiter()
    response = retry_get_url(url, num_retries=5, wait=wait_exponential_jitter(initial=0.5, max=60), headers=headers)
    response_json = response.json()
    total_events = response_json["message"]["total-results"]
    events = response_json["message"]["events"]
    next_cursor = response_json["message"]["next-cursor"]
    counter = len(events)

    return next_cursor, counter, total_events, events


@sleep_and_retry
@limits(calls=15, period=1)
def crossref_events_limiter():
    """ "Task to throttle the calls to the crossref events API"""
    return


def transform_crossref_events(events: List[dict], max_threads: int = 1) -> List[dict]:
    """
    Spawns workers to transforms crossref events

    :param all_events: A list of the events to transform
    :param max_threads: The maximum number of threads to utilise for the transforming process
    :return: transformed events, the order of the events in the input list is not preserved
    """
    print(f"Beginning crossref event transform with {max_threads} workers")
    transformed_events = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for event in events:
            futures.append(executor.submit(transform_event, event))
        for future in as_completed(futures):
            transformed_events.append(future.result())
    print("Crossref event transformation complete")
    return transformed_events


def transform_event(event: dict) -> dict:
    """Transform the dictionary with event data by replacing '-' with '_' in key names, converting all int values to
    string except for the 'total' field and parsing datetime columns for a valid datetime.

    :param event: The event dictionary
    :return: The transformed event dictionary
    """
    if isinstance(event, (str, int, float)):
        return event
    if isinstance(event, dict):
        new = event.__class__()
        for k, v in event.items():
            if isinstance(v, int) and k != "total":
                v = str(v)
            if k in ["timestamp", "occurred_at", "issued", "dateModified", "updated_date"]:
                try:
                    v = str(pendulum.parse(v))
                except ValueError:
                    v = "0001-01-01T00:00:00Z"

            # Replace hyphens with underscores for BigQuery compatibility
            k = k.replace("-", "_")

            # Replace @ symbol in keys left by DataCite between the 15 and 22 March 2019
            k = k.replace("@", "")

            new[k] = transform_event(v)
        return new


def create_latest_views_from_dataset(
    project_id: str, from_dataset: str, to_dataset: str, date_match: str, data_location: str, description: str = None
) -> None:
    """Creates views from all sharded tables from a dataset with a matching a date string.

    :param project_id: The project id
    :param from_dataset: The dataset containing the sharded tables
    :param to_dataset: The dataset to contain the views
    :param date_match: The date string to match. e.g. for a table named 'this_table20220101', this would be '20220101'
    :param data_location: The regional location of the data in google cloud
    :param description: The description for the views dataset
    """
    if description is None:
        description = "OAEBU Export Views for Dashboarding"
    # Make to_dataset if it doesn't exist
    bq_create_dataset(
        project_id=project_id,
        dataset_id=to_dataset,
        location=data_location,
        description=description,
    )

    # Get the tables from the from_dataset
    client = Client(project_id)
    tables = [t.table_id for t in client.list_tables(from_dataset)]

    # Find the tables with specified date string
    regex_string = rf"^\w+{date_match}\b"
    matched_tables = [re.findall(regex_string, t) for t in tables]
    matched_tables = [t[0] for t in matched_tables if t]
    assert len(matched_tables), f"No tables matching date {date_match} in dataset {project_id}.{from_dataset}"

    # Create all of the views
    for table in matched_tables:
        query = f"SELECT * FROM `{project_id}.{from_dataset}.{table}`"
        view_name = re.match(r"(.*?)\d{8}$", table).group(1)  # Drop the date from the table for the view name
        view_id = bq_table_id(project_id, to_dataset, view_name)
        bq_create_view(view_id=view_id, query=query)


def get_onix_records(table_id: str) -> List[dict]:
    """Fetch the latest onix snapshot from BigQuery.
    :param table_id: Fully qualified table ID.
    :return: List of onix product records.
    """

    sql = f"SELECT * FROM {table_id}"
    records = bq_run_query(sql)
    products = [{key: records[i][key] for key in records[i].keys()} for i in range(len(records))]
    return products


def get_isbn_utils_sql_string() -> str:
    """Load the ISBN utils sql functions.
    :return BQ SQL string.
    """

    isbn_utils_file = "isbn_utils.sql"
    isbn_utils_path = os.path.join(sql_folder(workflow_module="onix_workflow"), isbn_utils_file)
    with open(isbn_utils_path, "r") as f:
        isbn_utils_sql = f.read()

    return isbn_utils_sql
