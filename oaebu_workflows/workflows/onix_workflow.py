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
#
#
# Author: Tuan Chien & Richard Hosking

import os
import shutil
from datetime import timedelta
from functools import partial, update_wrapper
from pathlib import Path
from typing import List, Optional
import json
from urllib3.exceptions import MaxRetryError

from concurrent.futures import ThreadPoolExecutor, as_completed
import pendulum
from airflow.exceptions import AirflowException
from google.cloud.bigquery import SourceFormat

from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.config import sql_folder
from oaebu_workflows.workflows.oaebu_partners import OaebuPartner
from oaebu_workflows.workflows.onix_telescope import OnixTelescope
from oaebu_workflows.workflows.onix_work_aggregation import (
    BookWorkAggregator,
    BookWorkFamilyAggregator,
)
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.utils.file_utils import list_to_jsonl_gz
from observatory.platform.utils.url_utils import get_user_agent, retry_session
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    create_bigquery_dataset,
    create_bigquery_table_from_query,
    run_bigquery_query,
    select_table_shard_dates,
    upload_files_to_cloud_storage,
    upload_file_to_cloud_storage,
)
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.workflow_utils import (
    bq_load_shard,
    make_dag_id,
    make_release_date,
    make_table_name,
    table_ids_from_path,
)
from observatory.platform.workflows.workflow import AbstractRelease, Workflow
from observatory.platform.utils.api import make_observatory_api
from oaebu_workflows.seed.dataset_type_info import get_dataset_type_info
from oaebu_workflows.dag_tag import Tag

METADATA_FIELDS = [
    "DOI",
    "URL",
    "type",
    "title",
    "abstract",
    "issued",
    "ISSN",
    "ISBN",
    "issn-type",
    "publisher-location",
    "publisher",
    "member",
    "prefix",
    "container-title",
    "short-container-title",
    "group-title",
    "references-count",
    "is-referenced-by-count",
    "subject",
    "published-print",
    "license",
    "volume",
    "funder",
    "page",
    "author",
    "link",
    "clinical-trial-number",
    "alternative-id",
]
CROSSREF_METADATA_URL = (
    "https://api.crossref.org/works?filter=isbn:{isbn}"
    # Return columns
    f"&select={','.join(METADATA_FIELDS)}"
    # Max row return
    "&rows=1000"
)
# The metadata url template was made with reference to the crossref metadata api:
# https://api.crossref.org/swagger-ui/index.html#/Works/get_works

CROSSREF_EVENT_URL_TEMPLATE = (
    "https://api.eventdata.crossref.org/v1/events?mailto={mailto}"
    "&from-collected-date={start_date}&until-collected-date={end_date}&rows=1000"
    "&obj-id={doi}"
)
CROSSREF_EDITED_EVENT_URL_TEMPLATE = (
    "https://api.eventdata.crossref.org/v1/events/edited?mailto={mailto}"
    "&from-updated-date={start_date}&until-updated-date={end_date}&rows=1000"
    "&obj-id={doi}"
)
CROSSREF_DELETED_EVENT_URL_TEMPLATE = (
    "https://api.eventdata.crossref.org/v1/events/deleted?mailto={mailto}"
    "&from-updated-date={start_date}&until-updated-date={end_date}&rows=1000"
    "&obj-id={doi}"
)


class OnixWorkflowRelease(AbstractRelease):
    """
    Release information for OnixWorkflow.
    """

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
        onix_release_date: pendulum.DateTime,
        gcp_project_id: str,
        gcp_bucket_name: str,
        onix_dataset_id: str = "onix",
        onix_table_id: str = "onix",
        book_table_id: str = "book",
        book_product_table_id: str = "book_product",
        crossref_metadata_table_id: str = "crossref_metadata",
        crossref_events_table_id: str = "crossref_events",
        oaebu_data_qa_dataset: str = "oaebu_data_qa",
        workflow_dataset: str = "onix_workflow",
        oaebu_intermediate_dataset: str = "oaebu_intermediate",
        oaebu_dataset: str = "oaebu",
        oaebu_elastic_dataset: str = "data_export",
        worksid_table: str = "onix_workid_isbn",
        worksid_error_table: str = "onix_workid_isbn_errors",
        workfamilyid_table: str = "onix_workfamilyid_isbn",
        data_location: str = "us",
        dataset_description: str = "ONIX workflow tables",
        oaebu_intermediate_match_suffix: str = "_matched",
    ):
        """
        :param dag_id: DAG ID.
        :param release_date: The release date. It's the current execution date.
        :param onix_release_date: the ONIX release date.
        :param gcp_project_id: GCP Project ID.
        :param gcp_bucket_name: GCP bucket name.
        :param onix_dataset_id: GCP dataset ID for the onix data.
        :param onix_table_id: GCP table ID for the onix data.
        :param book_table_id: The name of the book table
        :param book_product_table_id: The name of the book product table
        :param crossref_metadata_table_id: The name of the corssref metadata table
        :param crossref_events_table_id: The name of the crossref events table
        :param oaebu_data_qa_dataset: OAEBU Data QA dataset.
        :param workflow_dataset: Onix workflow dataset.
        :param oaebu_intermediate_dataset: OAEBU intermediate dataset.
        :param oaebu_dataset: OAEBU dataset.
        :param oaebu_elastic_dataset: OAEBU elastic dataset.
        """

        self.dag_id = dag_id
        self.release_date = release_date
        self.onix_release_date = onix_release_date

        # Prepare filesystem
        self.worksid_table = worksid_table
        self.worksid_error_table = worksid_error_table
        self.workfamilyid_table = workfamilyid_table

        self.workslookup_filename = os.path.join(self.transform_folder, f"{self.worksid_table}.jsonl.gz")
        self.workslookup_errors_filename = os.path.join(self.transform_folder, f"{self.worksid_error_table}.jsonl.gz")
        self.worksfamilylookup_filename = os.path.join(self.transform_folder, f"{self.workfamilyid_table}.jsonl.gz")

        # GCP parameters
        self.project_id = gcp_project_id
        self.onix_dataset_id = onix_dataset_id
        self.data_location = data_location
        self.dataset_description = dataset_description

        # ONIX release info
        self.onix_table_id = onix_table_id
        self.bucket_name = gcp_bucket_name

        # Crossref release info and files
        self.crossref_metadata_table_id = crossref_metadata_table_id
        self.crossref_events_table_id = crossref_events_table_id
        self.crossref_metadata_filename = os.path.join(
            self.transform_folder, f"{self.crossref_metadata_table_id}_{release_date.strftime('%Y%m%d')}.jsonl.gz"
        )
        self.crossref_events_filename = os.path.join(
            self.transform_folder, f"{self.crossref_events_table_id}_{release_date.strftime('%Y%m%d')}.jsonl.gz"
        )

        Path("", self.transform_folder).mkdir(exist_ok=True, parents=True)

        self.book_table_id = book_table_id
        self.book_product_table_id = book_product_table_id
        self.oaebu_data_qa_dataset = oaebu_data_qa_dataset
        self.workflow_dataset = workflow_dataset
        self.oaebu_intermediate_dataset = oaebu_intermediate_dataset
        self.oaebu_dataset = oaebu_dataset
        self.oaebu_elastic_dataset = oaebu_elastic_dataset
        self.oaebu_intermediate_match_suffix = oaebu_intermediate_match_suffix

    @property
    def onix_table_id_sharded(self):
        return bigquery_sharded_table_id(self.onix_table_id, self.onix_release_date)

    @property
    def transform_bucket(self) -> str:
        """
        :return: The transform bucket name.
        """
        return self.bucket_name

    @property
    def transform_folder(self) -> str:
        """The transform folder path. This is the folder hierarchy locally and on GCP.
        :return: The transform folder path.
        """

        return os.path.join(str(self.dag_id), self.release_date.strftime("%Y%m%d"))

    @property
    def transform_files(self) -> List[str]:
        """
        :return: List of transformed files.
        """
        return [self.workslookup_filename, self.workslookup_errors_filename, self.worksfamilylookup_filename]

    @property
    def download_bucket(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def download_files(self) -> List[str]:
        """Not used.
        :return: Empty list.
        """
        return list()

    @property
    def extract_files(self) -> List[str]:
        """Not used.
        :return: Empty list.
        """
        return list()

    @property
    def download_folder(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def extract_folder(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    def cleanup(self):
        """Delete all files and folders associated with this release.
        :return: None.
        """
        shutil.rmtree(self.transform_folder)


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

    DAG_ID_PREFIX = "onix_workflow"

    def __init__(
        self,
        *,
        org_name: str,
        gcp_project_id: str,
        gcp_bucket_name: str,
        country_project_id: str = "oaebu-public-data",
        country_dataset_id: str = "oaebu_reference",
        onix_dataset_id: str = "onix",
        onix_table_id: str = "onix",
        crossref_dataset_id: str = "crossref",
        subject_project_id: str = "oaebu-public-data",
        subject_dataset_id: str = "oaebu_reference",
        schema_folder: str = default_schema_folder(),
        dag_id: Optional[str] = None,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2022, 8, 1),
        crossref_start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        data_partners: List[OaebuPartner] = None,
        workflow_id: int = None,
        mailto: str = "agent@observatory.academy",
        max_threads: int = os.cpu_count(),
    ):
        """Initialises the workflow object.
        :param org_name: Organisation name.
        :param gcp_project_id: Project ID in GCP.
        :param gcp_bucket_name: GCP bucket name to store files
        :param country_project_id: GCP project ID of the country table
        :param country_dataset_id: GCP dataset containing the country table
        :param onix_dataset_id: GCP dataset ID of the onix data.
        :param onix_table_id: GCP table ID of the onix data.
        :param crossref_dataset_id: GCP dataset ID of crossref data
        :param subject_project_id: GCP project ID of the subject tables
        :param subject_dataset_id: GCP dataset ID of the subject tables
        :param schema_folder: the SQL schema path.
        :param dag_id: DAG ID.
        :param start_date: Start date of the DAG.
        :param crossref_start_date: The starting date of crossref's API calls
        :param schedule_interval: Scheduled interval for running the DAG.
        :param catchup: Whether to catch up missed DAG runs.
        :param data_partners: OAEBU data sources.
        :param workflow_id: api workflow id.
        :param mailto: email address used to identify the user when sending requests to an api.
        :param max_threads: The maximum number of threads to use for parallel tasks.
        """

        self.dag_id = dag_id
        if dag_id is None:
            self.dag_id = make_dag_id(self.DAG_ID_PREFIX, org_name)

        if data_partners is None:
            data_partners = list()

        self.org_name = org_name
        self.gcp_project_id = gcp_project_id
        self.gcp_bucket_name = gcp_bucket_name
        self.onix_dataset_id = onix_dataset_id
        self.onix_table_id = onix_table_id
        self.subject_project_id = subject_project_id
        self.subject_dataset_id = subject_dataset_id
        self.schema_folder = schema_folder
        self.crossref_start_date = crossref_start_date
        self.mailto = mailto

        # Divide parallel processes
        self.max_threads = max_threads

        api = make_observatory_api()
        self.dataset_type_info = get_dataset_type_info(api)

        # Public Book Data
        self.country_project_id = country_project_id
        self.country_dataset_id = country_dataset_id
        self.crossref_dataset_id = crossref_dataset_id

        # Initialise Telesecope base class
        super().__init__(
            dag_id=self.dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            workflow_id=workflow_id,
            tags=[Tag.oaebu],
        )

        # Wait for data partner workflows to finish
        partner_data_dag_prefix_ids = [OnixTelescope.DAG_ID_PREFIX]
        partner_data_dag_prefix_ids.extend(list(set([partner.dag_id_prefix for partner in data_partners])))
        partner_data_dag_prefix_ids.sort()  # Sort so that order is deterministic
        with self.parallel_tasks():
            for dag_prefix in partner_data_dag_prefix_ids:
                ext_dag_id = make_dag_id(dag_prefix, org_name)
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
        self.add_task(self.bq_load_workid_lookup)
        self.add_task(self.bq_load_workid_lookup_errors)
        self.add_task(self.bq_load_workfamilyid_lookup)

        # Create crossref metadata and event tables
        self.add_task(self.create_oaebu_crossref_metadata_table)
        self.add_task(self.create_oaebu_crossref_events_table)

        # Create book table
        self.add_task(self.create_oaebu_book_table)

        # Create OAEBU Intermediate tables
        self.create_oaebu_intermediate_table_tasks(data_partners)

        # Create OAEBU tables
        self.create_oaebu_output_tasks(data_partners)

        # Create QA metrics tables
        self.create_oaebu_data_qa_tasks(data_partners)

        # Create OAEBU Elastic Export tables
        self.create_oaebu_export_tasks(data_partners)

        # Cleanup tasks
        self.add_task(self.cleanup)

        # Create DatasetRelease records
        self.add_task(self.add_new_dataset_releases)

    def get_isbn_utils_sql_string(self) -> str:
        """Load the ISBN utils sql functions.
        :return BQ SQL string.
        """

        isbn_utils_file = "isbn_utils.sql"
        isbn_utils_path = os.path.join(sql_folder(), isbn_utils_file)
        with open(isbn_utils_path, "r") as f:
            isbn_utils_sql = f.read()

        return isbn_utils_sql

    def make_release(self, **kwargs) -> OnixWorkflowRelease:
        """Creates a release object.
        :param kwargs: From Airflow. Contains the execution_date.
        :return: an OnixWorkflowRelease object.
        """

        # Make release date
        release_date = make_release_date(**kwargs)

        # Get ONIX release date
        onix_release_dates = select_table_shard_dates(
            project_id=self.gcp_project_id,
            dataset_id=self.onix_dataset_id,
            table_id=self.onix_table_id,
            end_date=release_date,
        )

        if not len(onix_release_dates):
            raise AirflowException("OnixWorkflow.make_release: no ONIX releases found")

        onix_release_date = onix_release_dates[0]
        onix_release_date = pendulum.datetime(onix_release_date.year, onix_release_date.month, onix_release_date.day)
        return OnixWorkflowRelease(
            dag_id=self.dag_id,
            release_date=release_date,
            gcp_project_id=self.gcp_project_id,
            gcp_bucket_name=self.gcp_bucket_name,
            onix_dataset_id=self.onix_dataset_id,
            onix_table_id=self.onix_table_id,
            onix_release_date=onix_release_date,
        )

    def get_onix_records(self, project_id: str, dataset_id: str, table_id: str) -> List[dict]:
        """Fetch the latest onix snapshot from BigQuery.
        :param project_id: Project ID.
        :param dataset_id: Dataset ID.
        :param table_id: Table ID.
        :return: List of onix product records.
        """

        sql = f"SELECT * FROM {project_id}.{dataset_id}.{table_id}"
        records = run_bigquery_query(sql)

        products = [{key: records[i][key] for key in records[i].keys()} for i in range(len(records))]

        return products

    def aggregate_works(self, release: OnixWorkflowRelease, **kwargs):
        """Fetches the ONIX product records from our ONIX database, aggregates them into works, workfamilies,
        and outputs it into jsonl files.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        # Fetch ONIX data
        products = self.get_onix_records(release.project_id, release.onix_dataset_id, release.onix_table_id_sharded)

        # Aggregate into works
        agg = BookWorkAggregator(products)
        works = agg.aggregate()
        lookup_table = agg.get_works_lookup_table()
        list_to_jsonl_gz(release.workslookup_filename, lookup_table)

        # Save errors from aggregation
        error_table = [{"Error": error} for error in agg.errors]
        list_to_jsonl_gz(release.workslookup_errors_filename, error_table)

        # Aggregate work families
        agg = BookWorkFamilyAggregator(works)
        works_families = agg.aggregate()
        lookup_table = agg.get_works_family_lookup_table()
        list_to_jsonl_gz(release.worksfamilylookup_filename, lookup_table)

    def upload_aggregation_tables(self, release: OnixWorkflowRelease, **kwargs):
        """Upload the aggregation tables and error tables to a GCP bucket in preparation for BQ loading.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        files = release.transform_files
        blobs = [os.path.join(release.transform_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=release.transform_bucket, blob_names=blobs, file_paths=files)

    def bq_load_workid_lookup(self, release: OnixWorkflowRelease, **kwargs):
        """Load the WorkID lookup table into BigQuery.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        blob = os.path.join(release.transform_folder, os.path.basename(release.workslookup_filename))
        table_id, _ = table_ids_from_path(release.workslookup_filename)
        schema_file_path = find_schema(path=self.schema_folder, table_name=table_id)
        bq_load_shard(
            schema_file_path=schema_file_path,
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob,
            dataset_id=release.workflow_dataset,
            data_location=release.data_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description=release.dataset_description,
            **{},
        )

    def bq_load_workid_lookup_errors(self, release: OnixWorkflowRelease, **kwargs):
        """Load the WorkID lookup table errors into BigQuery.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        blob = os.path.join(release.transform_folder, os.path.basename(release.workslookup_errors_filename))
        table_id, _ = table_ids_from_path(release.workslookup_errors_filename)
        schema_file_path = find_schema(path=self.schema_folder, table_name=table_id)
        bq_load_shard(
            schema_file_path=schema_file_path,
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob,
            dataset_id=release.workflow_dataset,
            data_location=release.data_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description=release.dataset_description,
            **{},
        )

    def bq_load_workfamilyid_lookup(self, release: OnixWorkflowRelease, **kwargs):
        """Load the WorkFamilyID lookup table into BigQuery.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        blob = os.path.join(release.transform_folder, os.path.basename(release.worksfamilylookup_filename))
        table_id, _ = table_ids_from_path(release.worksfamilylookup_filename)
        schema_file_path = find_schema(path=self.schema_folder, table_name=table_id)
        bq_load_shard(
            schema_file_path=schema_file_path,
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob,
            dataset_id=release.workflow_dataset,
            data_location=release.data_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description=release.dataset_description,
            **{},
        )

    def cleanup(self, release: OnixWorkflowRelease, **kwargs):
        """Cleanup temporary files.

        :param release: Workflow release objects.
        :param kwargs: Unused.
        """

        release.cleanup()

    def create_oaebu_crossref_metadata_table(self, release: OnixWorkflowRelease, **kwargs):
        """
        Download, transform, upload and create a table for crossref metadata

        :param release: The onix workflow release object
        """
        # Query the project's Onix table for ISBNs
        sharded_onix_id = bigquery_sharded_table_id(self.onix_table_id, release.onix_release_date)
        isbns = isbns_from_onix(self.gcp_project_id, self.onix_dataset_id, sharded_onix_id)

        # Download and tranfrom the metadata
        metadata = download_crossref_metadata(isbns, max_threads=self.max_threads)
        metadata = transform_crossref_metadata(metadata, max_threads=self.max_threads)

        # Zip and upload to google cloud
        list_to_jsonl_gz(
            release.crossref_metadata_filename,
            metadata,
        )  # Save metadata to gzipped jsonl file
        blob_name = os.path.join(
            release.transform_folder,
            os.path.basename(release.crossref_metadata_filename),
        )
        upload_file_to_cloud_storage(
            release.transform_bucket,
            blob_name,
            release.crossref_metadata_filename,
        )
        schema_file_path = find_schema(path=self.schema_folder, table_name=release.crossref_metadata_table_id)
        # load the table into bigquery
        bq_load_shard(
            schema_file_path=schema_file_path,
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob_name,
            dataset_id=self.crossref_dataset_id,
            data_location=release.data_location,
            table_id=release.crossref_metadata_table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description=release.dataset_description,
            **{},
        )

    def create_oaebu_crossref_events_table(self, release: OnixWorkflowRelease, **kwargs):
        """
        Download, transform, upload and create a table for crossref events

        :param release: The onix workflow release object
        """
        # Get the unique dois from the metadata table
        metadata_table_id = bigquery_sharded_table_id(release.crossref_metadata_table_id, release.release_date)
        dois = dois_from_crossref_metadata(release.project_id, self.crossref_dataset_id, metadata_table_id)

        # Download and transform all events
        start_date = self.crossref_start_date
        end_date = release.release_date.subtract(days=1).date()
        events = download_crossref_events(dois, start_date, end_date, self.mailto, max_threads=self.max_threads)
        events = transform_crossref_events(events, max_threads=self.max_threads)

        # Zip and upload to google cloud
        list_to_jsonl_gz(
            release.crossref_events_filename,
            events,
        )  # Save events to gzipped jsonl file
        blob_name = os.path.join(
            release.transform_folder,
            os.path.basename(release.crossref_events_filename),
        )
        upload_file_to_cloud_storage(
            release.transform_bucket,
            blob_name,
            release.crossref_events_filename,
        )
        schema_file_path = find_schema(path=self.schema_folder, table_name=release.crossref_events_table_id)

        # load the table into bigquery
        bq_load_shard(
            schema_file_path=schema_file_path,
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob_name,
            dataset_id=self.crossref_dataset_id,
            data_location=release.data_location,
            table_id=release.crossref_events_table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description=release.dataset_description,
            **{},
        )

    def create_oaebu_book_table(
        self,
        release: OnixWorkflowRelease,
        **kwargs,
    ):
        """
        Create the oaebu book table using the crossref event and metadata tables

        :params release: The onix workflow release object
        """
        output_table = bigquery_sharded_table_id(release.book_table_id, release.release_date)
        crossref_events_table_id = bigquery_sharded_table_id(release.crossref_events_table_id, release.release_date)
        crossref_metadata_table_id = bigquery_sharded_table_id(release.crossref_metadata_table_id, release.release_date)
        table_joining_template_file = "create_book.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)
        schema_file_path = find_schema(path=self.schema_folder, table_name=release.book_table_id)

        sql = render_template(
            template_path,
            project_id=release.project_id,
            crossref_dataset_id=self.crossref_dataset_id,
            crossref_events_table_id=crossref_events_table_id,
            crossref_metadata_table_id=crossref_metadata_table_id,
        )

        create_bigquery_dataset(
            project_id=release.project_id, dataset_id=release.oaebu_dataset, location=release.data_location
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=release.project_id,
            dataset_id=release.oaebu_dataset,
            table_id=output_table,
            location=release.data_location,
            schema_file_path=schema_file_path,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {release.project_id}.{release.oaebu_dataset}.{output_table}"
            )

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
        :param sharded: whether the table is sharded or not.
        """

        orig_table_id = make_table_name(
            project_id=orig_project_id,
            dataset_id=orig_dataset,
            table_id=orig_table,
            end_date=release.release_date,
            sharded=sharded,
        )

        output_table = f"{orig_dataset}_{orig_table}{release.oaebu_intermediate_match_suffix}"
        output_dataset = release.oaebu_intermediate_dataset

        data_location = release.data_location
        release_date = release.release_date
        table_joining_template_file = "assign_workid_workfamilyid.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)
        table_id = bigquery_sharded_table_id(output_table, release_date)
        dst_table_suffix = release_date.strftime("%Y%m%d")

        sql = render_template(
            template_path,
            project_id=orig_project_id,
            orig_dataset=orig_dataset,
            orig_table=orig_table_id,
            orig_isbn=orig_isbn,
            onix_workflow_dataset=release.workflow_dataset,
            wid_table=release.worksid_table + dst_table_suffix,
            wfam_table=release.workfamilyid_table + dst_table_suffix,
        )

        create_bigquery_dataset(project_id=release.project_id, dataset_id=output_dataset, location=data_location)

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=release.project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=release.data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{table_id}"
            )

    def create_oaebu_intermediate_table_tasks(self, data_partners: List[OaebuPartner]):
        """Create tasks for generating oaebu intermediate tables for each OAEBU data partner.
        :param data_partners: List of oaebu partner data.
        """

        with self.parallel_tasks():
            for data in data_partners:
                fn = partial(
                    self.create_oaebu_intermediate_table,
                    orig_project_id=data.gcp_project_id,
                    orig_dataset=data.gcp_dataset_id,
                    orig_table=data.gcp_table_id,
                    orig_isbn=data.isbn_field_name,
                    sharded=data.sharded,
                )

                # Populate the __name__ attribute of the partial object (it lacks one by default).
                # Scheme: create_oaebu_intermediate_table.dataset.table
                update_wrapper(fn, self.create_oaebu_intermediate_table)
                fn.__name__ += f".{data.gcp_dataset_id}.{data.gcp_table_id}"

                self.add_task(fn)

    def create_oaebu_book_product_table(
        self,
        release: OnixWorkflowRelease,
        *,
        include_google_analytics: bool,
        include_google_books: bool,
        include_jstor: bool,
        include_oapen: bool,
        include_ucl: bool,
        google_analytics_dataset: str,
        google_books_dataset: str,
        jstor_dataset: str,
        oapen_dataset: str,
        ucl_dataset: str,
        **kwargs,
    ):
        """Create the Book Product Table
        :param release: Onix workflow release information.
        :param include_google_analytics: Whether Google Analytics is a relevant data source for this publisher
        :param include_google_books: Whether Google Books is a relevant data source for this publisher
        :param include_jstor: Whether jstor is a relevant data source for this publisher
        :param include_oapen: Whether OAPEN is a relevant data source for this publisher
        :param include_ucl: Whether UCL Discovery is a relevant data source for this publisher
        :param google_analytics_dataset: dataset_id if it is a relevant data source for this publisher
        :param google_books_dataset: dataset_id if it is  a relevant data source for this publisher
        :param jstor_dataset: dataset_id if it is  a relevant data source for this publisher
        :param oapen_dataset: dataset_id if it is  a relevant data source for this publisher
        :param ucl_dataset: dataset_id if it is  a relevant data source for this publisher
        """

        project_id = release.project_id
        oaebu_intermediate_dataset = release.oaebu_intermediate_dataset

        data_location = release.data_location
        release_date = release.release_date

        book_table_id = bigquery_sharded_table_id(release.book_table_id, release_date)
        book_dataset_id = release.oaebu_dataset

        table_joining_template_file = "create_book_products.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)

        schema_file_path = find_schema(path=self.schema_folder, table_name=release.book_product_table_id)

        if include_google_analytics:
            google_analytics_table_id = f"{project_id}.{oaebu_intermediate_dataset}.{google_analytics_dataset}_google_analytics_matched{release_date.strftime('%Y%m%d')}"
        else:
            google_analytics_table_id = "empty_google_analytics"

        if include_google_books:
            google_books_sales_table_id = f"{project_id}.{oaebu_intermediate_dataset}.{google_books_dataset}_google_books_sales_matched{release_date.strftime('%Y%m%d')}"
            google_books_traffic_table_id = f"{project_id}.{oaebu_intermediate_dataset}.{google_books_dataset}_google_books_traffic_matched{release_date.strftime('%Y%m%d')}"
        else:
            google_books_sales_table_id = "empty_google_books_sales"
            google_books_traffic_table_id = "empty_google_books_traffic"

        if include_jstor:
            jstor_country_table_id = f"{project_id}.{oaebu_intermediate_dataset}.{jstor_dataset}_jstor_country_matched{release_date.strftime('%Y%m%d')}"
            jstor_institution_table_id = f"{project_id}.{oaebu_intermediate_dataset}.{jstor_dataset}_jstor_institution_matched{release_date.strftime('%Y%m%d')}"
        else:
            jstor_country_table_id = "empty_jstor_country"
            jstor_institution_table_id = "empty_jstor_institution"

        if include_oapen:
            oapen_table_id = f"{project_id}.{oaebu_intermediate_dataset}.{oapen_dataset}_oapen_irus_uk_matched{release_date.strftime('%Y%m%d')}"
        else:
            oapen_table_id = "empty_oapen"

        if include_ucl:
            ucl_table_id = f"{project_id}.{oaebu_intermediate_dataset}.{ucl_dataset}_ucl_discovery_matched{release_date.strftime('%Y%m%d')}"
        else:
            ucl_table_id = "empty_ucl_discovery"

        sql = render_template(
            template_path,
            project_id=release.project_id,
            onix_dataset_id=release.onix_dataset_id,
            dataset_id=release.oaebu_intermediate_dataset,
            onix_release_date=release.onix_release_date,
            release_date=release_date,
            onix_workflow=True,
            onix_workflow_dataset=release.workflow_dataset,
            google_analytics_table_id=google_analytics_table_id,
            google_books_sales_table_id=google_books_sales_table_id,
            google_books_traffic_table_id=google_books_traffic_table_id,
            jstor_country_table_id=jstor_country_table_id,
            jstor_institution_table_id=jstor_institution_table_id,
            oapen_table_id=oapen_table_id,
            ucl_table_id=ucl_table_id,
            book_table_id=book_table_id,
            book_dataset_id=book_dataset_id,
        )

        output_dataset = release.oaebu_dataset
        table_id = bigquery_sharded_table_id(release.book_product_table_id, release_date)

        create_bigquery_dataset(project_id=release.project_id, dataset_id=output_dataset, location=data_location)

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=release.project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=release.data_location,
            schema_file_path=schema_file_path,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{table_id}"
            )

    def create_oaebu_output_tasks(self, data_partners: List[OaebuPartner]):
        """Create tasks for outputing final metrics from our OAEBU data.  It will create output tables in the oaebu dataset.
        :param data_partners: List of oaebu partner data.
        """

        data_partner_datasets = {data.dataset_type_id: data.gcp_dataset_id for data in data_partners}

        # Book Product
        fn = partial(
            self.create_oaebu_book_product_table,
            include_google_analytics=self.dataset_type_info["google_analytics"].type_id in data_partner_datasets,
            include_google_books=self.dataset_type_info["google_books_traffic"].type_id in data_partner_datasets,
            include_jstor=self.dataset_type_info["jstor_country"].type_id in data_partner_datasets,
            include_oapen=self.dataset_type_info["oapen_irus_uk"].type_id in data_partner_datasets,
            include_ucl=self.dataset_type_info["ucl_discovery"].type_id in data_partner_datasets,
            google_analytics_dataset=data_partner_datasets.get(
                self.dataset_type_info["google_analytics"].type_id, None
            ),
            google_books_dataset=data_partner_datasets.get(
                self.dataset_type_info["google_books_traffic"].type_id, None
            ),
            jstor_dataset=data_partner_datasets.get(self.dataset_type_info["jstor_country"].type_id, None),
            oapen_dataset=data_partner_datasets.get(self.dataset_type_info["oapen_irus_uk"].type_id, None),
            ucl_dataset=data_partner_datasets.get(self.dataset_type_info["ucl_discovery"].type_id, None),
        )

        # Populate the __name__ attribute of the partial object (it lacks one by default).
        # Scheme: create_oaebu_table.dataset.table
        update_wrapper(fn, self.create_oaebu_book_product_table)

        self.add_task(fn)

    def export_oaebu_table(
        self,
        release: OnixWorkflowRelease,
        *,
        output_table: str,
        query_template: str,
        **kwargs,
    ):
        """Create an intermediate oaebu table.  They are of the form datasource_matched<date>
        :param release: Onix workflow release information.
        """

        output_dataset = release.oaebu_elastic_dataset
        data_location = release.data_location
        release_date = release.release_date

        create_bigquery_dataset(project_id=release.project_id, dataset_id=output_dataset, location=data_location)

        table_id = bigquery_sharded_table_id(f"{release.project_id.replace('-', '_')}_{output_table}", release_date)
        template_path = os.path.join(sql_folder(), query_template)
        schema_file_path = find_schema(
            path=self.schema_folder,
            table_name=output_table,
            prefix="oaebu_publisher_",
        )

        sql = render_template(
            template_path,
            project_id=release.project_id,
            dataset_id=release.oaebu_dataset,
            release=release_date,
            country_project_id=self.country_project_id,
            country_dataset_id=self.country_dataset_id,
            subject_project_id=self.subject_project_id,
            subject_dataset_id=self.subject_dataset_id,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=release.project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=release.data_location,
            schema_file_path=schema_file_path,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{table_id}"
            )

    def export_oaebu_qa_metrics(
        self,
        release: OnixWorkflowRelease,
        *,
        include_google_analytics: bool,
        include_google_books: bool,
        include_jstor: bool,
        include_oapen: bool,
        include_ucl: bool,
        google_analytics_table: str,
        google_books_table: str,
        jstor_table: str,
        oapen_table: str,
        ucl_table: str,
        google_analytics_isbn: str,
        google_books_isbn: str,
        jstor_isbn: str,
        oapen_isbn: str,
        ucl_isbn: str,
        **kwargs,
    ):
        """Create the unmatched metrics table
        :param release: Onix workflow release information.
        :param include_google_analytics: Whether Google Analytics is a relevant data source for this publisher
        :param include_google_books: Whether Google Books is a relevant data source for this publisher
        :param include_jstor: Whether jstor is a relevant data source for this publisher
        :param include_oapen: Whether OAPEN is a relevant data source for this publisher
        :param include_ucl: Whether UCL Discovery is a relevant data source for this publisher
        :param google_analytics_table: table_id if it is a relevant data source for this publisher
        :param google_books_table: table_id if it is  a relevant data source for this publisher
        :param jstor_table: table_id if it is  a relevant data source for this publisher
        :param oapen_table: table_id if it is  a relevant data source for this publisher
        :param ucl_table: table_id if it is  a relevant data source for this publisher
        :param google_analytics_isbn: isbn field if it is a relevant data source for this publisher
        :param google_books_isbn: isbn field if it is  a relevant data source for this publisher
        :param jstor_isbn: isbn field if it is  a relevant data source for this publisher
        :param oapen_isbn: isbn field if it is  a relevant data source for this publisher
        :param ucl_isbn: isbn field if it is  a relevant data source for this publisher
        """

        output_dataset = release.oaebu_elastic_dataset
        data_location = release.data_location
        release_date = release.release_date

        create_bigquery_dataset(project_id=release.project_id, dataset_id=output_dataset, location=data_location)

        # Un-matched Metrics
        output_table = "unmatched_book_metrics"
        table_id = bigquery_sharded_table_id(f"{release.project_id.replace('-', '_')}_{output_table}", release_date)
        table_joining_template_file = "export_unmatched_metrics.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)

        sql = render_template(
            template_path,
            project_id=release.project_id,
            dataset_id=release.oaebu_data_qa_dataset,
            release=release_date,
            google_analytics=include_google_analytics,
            google_books=include_google_books,
            jstor=include_jstor,
            oapen=include_oapen,
            ucl_discovery=include_ucl,
            google_analytics_table=google_analytics_table,
            google_books_table=google_books_table,
            jstor_table=jstor_table,
            oapen_table=oapen_table,
            ucl_table=ucl_table,
            google_analytics_isbn=google_analytics_isbn,
            google_books_isbn=google_books_isbn,
            jstor_isbn=jstor_isbn,
            oapen_isbn=oapen_isbn,
            ucl_isbn=ucl_isbn,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=release.project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=release.data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{table_id}"
            )

    def create_oaebu_export_tasks(self, data_partners: List[OaebuPartner]):
        """Create tasks for exporting final metrics from our OAEBU data.  It will create output tables in the oaebu_elastic dataset.
        :param data_partners: Onix workflow release information.
        """

        export_tables = [
            {"output_table": "book_product_list", "query_template": "export_book_list.sql.jinja2", "file_type": "json"},
            {
                "output_table": "book_product_metrics",
                "query_template": "export_book_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_country",
                "query_template": "export_book_metrics_country.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_institution",
                "query_template": "export_book_metrics_institution.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "institution_list",
                "query_template": "export_institution_list.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_city",
                "query_template": "export_book_metrics_city.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_events",
                "query_template": "export_book_metrics_event.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_publisher_metrics",
                "query_template": "export_book_publisher_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_bic_metrics",
                "query_template": "export_book_subject_bic_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_bisac_metrics",
                "query_template": "export_book_subject_bisac_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_thema_metrics",
                "query_template": "export_book_subject_thema_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_year_metrics",
                "query_template": "export_book_year_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_year_metrics",
                "query_template": "export_book_subject_year_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_author_metrics",
                "query_template": "export_book_author_metrics.sql.jinja2",
                "file_type": "json",
            },
        ]

        # Create each export table in BiqQuery
        with self.parallel_tasks():
            for export_table in export_tables:
                fn = partial(
                    self.export_oaebu_table,
                    output_table=export_table["output_table"],
                    query_template=export_table["query_template"],
                    schema_file_path=default_schema_folder(),
                )

                # Populate the __name__ attribute of the partial object (it lacks one by default).
                update_wrapper(fn, self.export_oaebu_table)
                fn.__name__ += f".{export_table['output_table']}"

                self.add_task(fn)

        # Export QA Metrics
        data_partner_tables = {data.dataset_type_id: data.gcp_table_id for data in data_partners}
        data_partner_isbns = {data.dataset_type_id: data.isbn_field_name for data in data_partners}

        # Export QA Metrics
        fn = partial(
            self.export_oaebu_qa_metrics,
            include_google_analytics=self.dataset_type_info["google_analytics"].type_id in data_partner_tables,
            include_google_books=self.dataset_type_info["google_books_traffic"].type_id in data_partner_tables,
            include_jstor=self.dataset_type_info["jstor_country"].type_id in data_partner_tables,
            include_oapen=self.dataset_type_info["oapen_irus_uk"].type_id in data_partner_tables,
            include_ucl=self.dataset_type_info["ucl_discovery"].type_id in data_partner_tables,
            google_analytics_table=data_partner_tables.get(self.dataset_type_info["google_analytics"].type_id, None),
            google_books_table=data_partner_tables.get(self.dataset_type_info["google_books_traffic"].type_id, None),
            jstor_table=data_partner_tables.get(self.dataset_type_info["jstor_country"].type_id, None),
            oapen_table=data_partner_tables.get(self.dataset_type_info["oapen_irus_uk"].type_id, None),
            ucl_table=data_partner_tables.get(self.dataset_type_info["ucl_discovery"].type_id, None),
            google_analytics_isbn=data_partner_isbns.get(self.dataset_type_info["google_analytics"].type_id, None),
            google_books_isbn=data_partner_isbns.get(self.dataset_type_info["google_books_traffic"].type_id, None),
            jstor_isbn=data_partner_isbns.get(self.dataset_type_info["jstor_country"].type_id, None),
            oapen_isbn=data_partner_isbns.get(self.dataset_type_info["oapen_irus_uk"].type_id, None),
            ucl_isbn=data_partner_isbns.get(self.dataset_type_info["ucl_discovery"].type_id, None),
        )

        # Populate the __name__ attribute of the partial object (it lacks one by default).
        # Scheme: create_oaebu_table.dataset.table
        update_wrapper(fn, self.export_oaebu_qa_metrics)
        self.add_task(fn)

    def create_oaebu_data_qa_tasks(self, data_partners: List[OaebuPartner]):
        """Create tasks for outputing QA metrics from our OAEBU data.  It will create output tables in the oaebu_data_qa dataset.
        :param data_partners: List of oaebu partner data.
        """

        with self.parallel_tasks():
            self.create_oaebu_data_qa_onix()

            for data_partner in data_partners:

                if (
                    data_partner.dataset_type_id == self.dataset_type_info["jstor_country"].type_id
                    or data_partner.dataset_type_id == self.dataset_type_info["jstor_institution"].type_id
                ):
                    self.create_oaebu_data_qa_jstor_tasks(data_partner)
                elif data_partner.dataset_type_id == self.dataset_type_info["oapen_irus_uk"].type_id:
                    self.create_oaebu_data_qa_oapen_irus_uk_tasks(data_partner)
                elif data_partner.dataset_type_id == self.dataset_type_info["google_books_sales"].type_id:
                    self.create_oaebu_data_qa_google_books_sales_tasks(data_partner)
                elif data_partner.dataset_type_id == self.dataset_type_info["google_books_traffic"].type_id:
                    self.create_oaebu_data_qa_google_books_traffic_tasks(data_partner)
                elif data_partner.dataset_type_id == self.dataset_type_info["google_analytics"].type_id:
                    self.create_oaebu_data_qa_google_analytics_tasks(data_partner)

                self.create_oaebu_data_qa_intermediate_tasks(data_partner)

    def create_oaebu_data_qa_onix(self, *args, **kwargs):
        """Create ONIX quality assurance metrics."""

        # isbn validation
        self.add_task(self.create_oaebu_data_qa_onix_isbn)

        # aggregate metrics
        self.add_task(self.create_oaebu_data_qa_onix_aggregate)

    def create_oaebu_data_qa_onix_aggregate(self, release: OnixWorkflowRelease, *args, **kwargs):
        """Create a bq table of some aggregate metrics for the ONIX data set.
        :param release: workflow release.
        """

        template_file = "onix_aggregate_metrics.sql.jinja2"
        template_path = os.path.join(sql_folder(), template_file)

        onix_project_id = release.project_id
        onix_dataset_id = release.onix_dataset_id
        onix_table_id = release.onix_table_id_sharded
        output_dataset = release.oaebu_data_qa_dataset
        output_table = "onix_aggregate_metrics"
        release_date = release.release_date
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        schema_file_path = find_schema(self.schema_folder, output_table)

        sql = render_template(
            template_path,
            project_id=onix_project_id,
            dataset_id=onix_dataset_id,
            table_id=onix_table_id,
            isbn="ISBN13",
        )

        create_bigquery_dataset(
            project_id=release.project_id,
            dataset_id=release.oaebu_data_qa_dataset,
            location=release.data_location,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=release.project_id,
            dataset_id=release.oaebu_data_qa_dataset,
            table_id=output_table_id,
            location=release.data_location,
            schema_file_path=schema_file_path,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{output_table_id}"
            )

    def create_oaebu_data_qa_onix_isbn(self, release: OnixWorkflowRelease, *args, **kwargs):
        """Create a BQ table of invalid ISBNs for the ONIX feed that can be fed back to publishers.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param release: Onix workflow release.
        """

        release_date = release.release_date
        project_id = release.project_id
        orig_dataset_id = release.onix_dataset_id
        orig_table_id = bigquery_sharded_table_id(release.onix_table_id, release.onix_release_date)
        output_dataset_id = release.oaebu_data_qa_dataset
        output_table = "onix_invalid_isbn"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        data_location = release.data_location
        schema_file_path = find_schema(self.schema_folder, output_table)

        self.oaebu_data_qa_validate_isbn(
            project_id=project_id,
            orig_dataset_id=orig_dataset_id,
            orig_table_id=orig_table_id,
            output_dataset_id=output_dataset_id,
            output_table_id=output_table_id,
            data_location=data_location,
            isbn="ISBN13",
            schema_file_path=schema_file_path,
        )

    def oaebu_data_qa_validate_isbn(
        self,
        *,
        project_id: str,
        orig_dataset_id,
        orig_table_id: str,
        output_dataset_id: str,
        output_table_id: str,
        data_location: str,
        isbn: str,
        schema_file_path: str = None,
    ):
        """Create a BQ table of invalid ISBNs for the ONIX feed that can be fed back to publishers.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param project_id: GCP Project ID of the data.
        :param orig_dataset_id: GCP Dataset ID of the source data.
        :param orig_table_id: Table ID of the source data (excluding date).
        :param output_dataset_id: Dataset ID for the output data.
        :param output_table_id: Table ID for the output data.
        :apram data_location: Location of GCP servers.
        :param isbn: Name of the isbn field in source table.
        :param schema_file_path: The path of the schema file to use for the BigQuery upload
        """

        isbn_utils_sql = self.get_isbn_utils_sql_string()

        isbn_validate_template_file = "validate_isbn.sql.jinja2"
        template_path = os.path.join(sql_folder(), isbn_validate_template_file)

        sql = render_template(
            template_path,
            project_id=project_id,
            dataset_id=orig_dataset_id,
            table_id=orig_table_id,
            isbn=isbn,
        )

        sql = isbn_utils_sql + sql

        create_bigquery_dataset(
            project_id=project_id,
            dataset_id=output_dataset_id,
            location=data_location,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset_id,
            table_id=output_table_id,
            location=data_location,
            schema_file_path=schema_file_path,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset_id}.{output_table_id}"
            )

    def create_oaebu_data_qa_jstor_tasks(self, data_partner: OaebuPartner):
        """Create JSTOR quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """

        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_jstor_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            sharded=data_partner.sharded,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_jstor_isbn)
        fn.__name__ += f".{data_partner.gcp_dataset_id}.{data_partner.gcp_table_id}"
        self.add_task(fn)

    def create_oaebu_data_qa_jstor_isbn(
        self,
        release: OnixWorkflowRelease,
        *,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        sharded: bool,
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the JSTOR feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param release: workflow release object.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        :param sharded: whether the table is sharded or not.
        """

        release_date = release.release_date
        orig_table_id = make_table_name(
            project_id=project_id,
            dataset_id=orig_dataset_id,
            table_id=orig_table,
            end_date=release_date,
            sharded=sharded,
        )

        # select table suffixes to get table suffix
        output_dataset_id = release.oaebu_data_qa_dataset

        # Validate the ISBN field
        output_table = "jstor_invalid_isbn"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        data_location = release.data_location
        self.oaebu_data_qa_validate_isbn(
            project_id=project_id,
            orig_dataset_id=orig_dataset_id,
            orig_table_id=orig_table_id,
            output_dataset_id=output_dataset_id,
            output_table_id=output_table_id,
            data_location=data_location,
            isbn="ISBN",
        )

        # Validate the eISBN field
        output_table = "jstor_invalid_eisbn"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        self.oaebu_data_qa_validate_isbn(
            project_id=project_id,
            orig_dataset_id=orig_dataset_id,
            orig_table_id=orig_table_id,
            output_dataset_id=output_dataset_id,
            output_table_id=output_table_id,
            data_location=data_location,
            isbn="eISBN",
        )

    def create_oaebu_data_qa_google_analytics_tasks(self, data_partner: OaebuPartner):
        """Create Google Analytics quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """
        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_google_analytics_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            sharded=data_partner.sharded,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_google_analytics_isbn)
        self.add_task(fn)

    def create_oaebu_data_qa_google_analytics_isbn(
        self,
        release: OnixWorkflowRelease,
        *,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        sharded: bool,
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the Google Analytics feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param release: workflow release object.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        :param sharded: whether the table is sharded or not.
        """

        release_date = release.release_date
        orig_table_id = make_table_name(
            project_id=project_id,
            dataset_id=orig_dataset_id,
            table_id=orig_table,
            end_date=release_date,
            sharded=sharded,
        )

        # select table suffixes to get table suffix
        output_dataset_id = release.oaebu_data_qa_dataset

        # Validate the ISBN field
        output_table = "google_analytics_invalid_isbn"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        data_location = release.data_location
        self.oaebu_data_qa_validate_isbn(
            project_id=project_id,
            orig_dataset_id=orig_dataset_id,
            orig_table_id=orig_table_id,
            output_dataset_id=output_dataset_id,
            output_table_id=output_table_id,
            data_location=data_location,
            isbn="publication_id",
        )

    def create_oaebu_data_qa_oapen_irus_uk_tasks(self, data_partner: OaebuPartner):
        """Create OAPEN IRUS UK quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """

        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_oapen_irus_uk_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            sharded=data_partner.sharded,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_oapen_irus_uk_isbn)
        self.add_task(fn)

    def create_oaebu_data_qa_oapen_irus_uk_isbn(
        self,
        release: OnixWorkflowRelease,
        *,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        sharded: bool,
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the OAPEN IRUS UK feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param release: workflow release object.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        :param sharded: whether the table is sharded or not.
        """

        release_date = release.release_date
        orig_table_id = make_table_name(
            project_id=project_id,
            dataset_id=orig_dataset_id,
            table_id=orig_table,
            end_date=release_date,
            sharded=sharded,
        )

        # select table suffixes to get table suffix
        output_dataset_id = release.oaebu_data_qa_dataset

        # Validate the ISBN field
        output_table = "oapen_irus_uk_invalid_isbn"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        data_location = release.data_location
        self.oaebu_data_qa_validate_isbn(
            project_id=project_id,
            orig_dataset_id=orig_dataset_id,
            orig_table_id=orig_table_id,
            output_dataset_id=output_dataset_id,
            output_table_id=output_table_id,
            data_location=data_location,
            isbn="ISBN",
        )

    def create_oaebu_data_qa_google_books_sales_tasks(self, data_partner: OaebuPartner):
        """Create Google Books Sales quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """

        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_google_books_sales_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            sharded=data_partner.sharded,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_google_books_sales_isbn)
        self.add_task(fn)

    def create_oaebu_data_qa_google_books_sales_isbn(
        self,
        release: OnixWorkflowRelease,
        *,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        sharded: bool,
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the Google Books Sales feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param release: List of workflow release objects.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        :param sharded: whether the table is sharded or not.
        """

        release_date = release.release_date
        orig_table_id = make_table_name(
            project_id=project_id,
            dataset_id=orig_dataset_id,
            table_id=orig_table,
            end_date=release_date,
            sharded=sharded,
        )

        # select table suffixes to get table suffix
        output_dataset_id = release.oaebu_data_qa_dataset

        # Validate the ISBN field
        output_table = "google_books_sales_invalid_isbn"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        data_location = release.data_location
        self.oaebu_data_qa_validate_isbn(
            project_id=project_id,
            orig_dataset_id=orig_dataset_id,
            orig_table_id=orig_table_id,
            output_dataset_id=output_dataset_id,
            output_table_id=output_table_id,
            data_location=data_location,
            isbn="Primary_ISBN",
        )

    def create_oaebu_data_qa_google_books_traffic_tasks(self, data_partner: OaebuPartner):
        """Create Google Books Traffic quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """

        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_google_books_traffic_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            sharded=data_partner.sharded,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_google_books_traffic_isbn)
        self.add_task(fn)

    def create_oaebu_data_qa_google_books_traffic_isbn(
        self,
        release: OnixWorkflowRelease,
        *,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        sharded: bool,
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the Google Books Traffic feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param release: workflow release object.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        :param sharded: whether the table is sharded or not.
        """

        release_date = release.release_date
        orig_table_id = make_table_name(
            project_id=project_id,
            dataset_id=orig_dataset_id,
            table_id=orig_table,
            end_date=release_date,
            sharded=sharded,
        )

        # select table suffixes to get table suffix
        output_dataset_id = release.oaebu_data_qa_dataset

        # Validate the ISBN field
        output_table = "google_books_traffic_invalid_isbn"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)
        data_location = release.data_location
        self.oaebu_data_qa_validate_isbn(
            project_id=project_id,
            orig_dataset_id=orig_dataset_id,
            orig_table_id=orig_table_id,
            output_dataset_id=output_dataset_id,
            output_table_id=output_table_id,
            data_location=data_location,
            isbn="Primary_ISBN",
        )

    def create_oaebu_data_qa_intermediate_tasks(self, data_partner: OaebuPartner):
        """Create tasks for generating oaebu intermediate metrics for each OAEBU data partner.
        :param data_partner: List of oaebu partner data.
        """

        # Record unmatched work_id. This is indicative of invalid ISBN or missing ONIX product records.

        fn = partial(
            self.create_oaebu_data_qa_intermediate_unmatched_workid,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            orig_isbn=data_partner.isbn_field_name,
            orig_title=data_partner.title_field_name,
        )

        # Populate the __name__ attribute of the partial object (it lacks one by default).
        # Scheme: create_oaebu_intermediate_table.dataset.table
        update_wrapper(fn, self.create_oaebu_data_qa_intermediate_unmatched_workid)
        fn.__name__ += f".{data_partner.gcp_dataset_id}.{data_partner.gcp_table_id}"

        self.add_task(fn)

        # Other tasks

    def create_oaebu_data_qa_intermediate_unmatched_workid(
        self,
        release: OnixWorkflowRelease,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        orig_isbn: str,
        orig_title: str,
        *args,
        **kwargs,
    ):
        """Create quality assurance metrics for the OAEBU intermediate tables.
        :param release: workflow release object.
        :param project_id: GCP Project ID for the data.
        :param orig_dataset_id: Dataset ID of the original partner data.
        :param orig_table: Original table name for the partner data.
        :param orig_isbn: Name of the ISBN field we're checking.
        :param orig_title: Name of the title field we're including.
        """

        template_file = "oaebu_intermediate_metrics.sql.jinja2"
        template_path = os.path.join(sql_folder(), template_file)

        release_date = release.release_date
        intermediate_table_id = (
            f"{orig_dataset_id}_{orig_table}{release.oaebu_intermediate_match_suffix}{release_date.strftime('%Y%m%d')}"
        )
        output_dataset = release.oaebu_data_qa_dataset
        output_table = f"{orig_table}_unmatched_{orig_isbn}"
        output_table_id = bigquery_sharded_table_id(output_table, release_date)

        sql = render_template(
            template_path,
            project_id=project_id,
            dataset_id=release.oaebu_intermediate_dataset,
            table_id=intermediate_table_id,
            isbn=orig_isbn,
            title=orig_title,
        )

        create_bigquery_dataset(
            project_id=release.project_id,
            dataset_id=release.oaebu_data_qa_dataset,
            location=release.data_location,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=release.project_id,
            dataset_id=release.oaebu_data_qa_dataset,
            table_id=output_table_id,
            location=release.data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{output_table_id}"
            )


def isbns_from_onix(project_id: str, onix_dataset_id: str, onix_table_id: str) -> List[str]:
    """
    Queries an Onix table to obtain its unique ISBNs

    :param project_id: The GCP project containing the relevant Onix dataset
    :param onix_dataset_id: The GCP dataset containing the relevant Onix table
    :param onix_talbe_id: The GCP Onix table name
    """
    sql = f"""
    SELECT
        DISTINCT(ISBN13)
    FROM
        `{project_id}.{onix_dataset_id}.{onix_table_id}`
    """
    isbn_rows = run_bigquery_query(sql)
    isbns = [row["ISBN13"] for row in isbn_rows]
    isbns = list(filter(None, isbns))  # Remove null values
    return isbns[:200]


def dois_from_onix(project_id: str, onix_dataset_id: str, onix_table_id: str) -> List[str]:
    """
    Queries an Onix table to obtain its unique DOIs.
    This is only useful for oapen data as its DOI column is filled.

    :param project_id: The GCP project containing the relevant Onix dataset
    :param onix_dataset_id: The GCP dataset containing the relevant Onix table
    :param onix_talbe_id: The GCP Onix table name
    """
    sql = f"""
    SELECT
        DISTINCT(Doi)
    FROM
        `{project_id}.{onix_dataset_id}.{onix_table_id}`
    """
    doi_rows = run_bigquery_query(sql)
    dois = [row["Doi"] for row in doi_rows]
    dois = list(filter(None, dois))  # Remove null values
    return dois


def download_crossref_metadata(isbns: List[str], max_threads: int = 1) -> List[dict]:
    """
    Spawns multiple threads to download metadata (DOI and publisher only) for each isbn supplied.

    :param isbns: The list of ISBNs to grab metadata for
    :param max_threads: The maximum number of threads to split the download tasks into
    """
    dois = []
    print(f"Beginning crossref metadata download from {len(isbns)} ISBNs with {max_threads} workers")
    print(
        "Downloading ISBN data using URL (with substitued ISBN values):" f"{CROSSREF_METADATA_URL.format(isbn='***')}"
    )
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i, isbn in enumerate(isbns):
            url = CROSSREF_METADATA_URL.format(isbn=isbn)
            futures.append(executor.submit(download_crossref_isbn_metadata, url, i=i))
        for future in as_completed(futures):
            dois.extend(future.result())

    # The API will return x results for each DOI where x is the number of ISBNS associated with that DOI
    # Effectively, if a DOI has 2 ISBNs, we will get 2 identical returns for that DOI
    # Deduplicate the result:
    deduped_dois = []
    for i in range(len(dois)):
        if dois[i] not in dois[i + 1 :]:
            deduped_dois.append(dois[i])
    return deduped_dois


def download_crossref_isbn_metadata(url: str, i: int = 0):
    """
    Downloads all crossref metadata from a url, iterating through pages if there is more than one
    The metadata API will return the same cursor for each unique request, but the cursor will point to a new page each
    time the request is made.

    :param url: The url send the request to
    :param i: Worker number
    """
    metadata = []
    headers = {"User-Agent": get_user_agent(package_name="oaebu_workflows")}
    cursor = "*"
    while True:
        tmp_url = url + f"&cursor={cursor}"
        cursor, isbn_metadata = download_crossref_page_metadata(tmp_url, headers, i=i)
        metadata.extend(isbn_metadata)
        if not isbn_metadata:
            # Break out of the loop when new data stops being returned
            break

    print(f"{i + 1}: {url} successful")
    return metadata


def download_crossref_page_metadata(url: str, headers: dict, i: int = 0):
    """
    Sends a request to a url and extends the result to the input list.
    On rare occassions, we get back a dictionary in a different format. There's a retry loop for this case.

    :param url: The url to send the request to
    :headers: Headers to attach to the request
    :i: The process number
    """
    print(f"{i+1}: Retrieving ISBN data from: {url}")
    try:
        response = retry_session(num_retries=5, backoff_factor=0.8).get(url, headers=headers)
    except MaxRetryError:
        raise ConnectionError(f"{i+1}: Error downloading file from {url}, status_code={response.status_code}")
    # Check if authorisation with the api token was successful or not, raise error if not successful
    response_json = json.loads(response.content.decode("utf-8"))
    isbn_metadata = response_json["message"]["items"]
    next_cursor = response_json["message"]["next-cursor"]
    return next_cursor, isbn_metadata


def transform_crossref_metadata(metadata: List[dict], max_threads: int = 1) -> List[dict]:
    """
    Spawns workers to transform crossref metadata

    :param all_events: A list of crossref metadata items to transform
    :return: transformed metadata, the order of the items in the input list is not preserved
    """
    print(f"Beginning crossref metadata transform with {max_threads} workers")
    transformed_metadata = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for item in metadata:
            futures.append(executor.submit(transform_metadata_item, item))
        for future in as_completed(futures):
            transformed_metadata.append(future.result())
    print("Crossref metadata transform complete")
    return transformed_metadata


def transform_metadata_item(item: dict) -> dict:
    """
    Transform a single Crossref Metadata JSON dictionary

    :param item: a JSON dictionary.
    """

    if isinstance(item, dict):
        new = {}
        for k, v in item.items():
            # Replace hyphens with underscores for BigQuery compatibility
            k = k.replace("-", "_")

            # Get inner array for date parts
            if k == "date_parts":
                v = v[0]
                if None in v:
                    # "date-parts" : [ [ null ] ]
                    v = []

            new[k] = transform_metadata_item(v)
        return new
    elif isinstance(item, list):
        return [transform_metadata_item(i) for i in item]
    else:
        return item


def dois_from_crossref_metadata(project_id: str, dataset_id: str, metadata_table_id: str) -> List[str]:
    """
    Queries a crossref metadata table to retrieve the unique DOIs

    :param project_id: The GCP project ID
    :param dataset_id: The GCP dataset ID
    :param metadata_table_id: The ID of the metadata table on GCP
    """
    sql = f"SELECT DISTINCT(DOI) FROM `{project_id}.{dataset_id}.{metadata_table_id}`"
    query_results = run_bigquery_query(sql)
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

    :param release: The onix workflow release object
    :param dois: The list of DOIs to download the events for
    """
    event_url_template = CROSSREF_EVENT_URL_TEMPLATE
    edited_event_url_template = CROSSREF_EDITED_EVENT_URL_TEMPLATE
    deleted_event_url_template = CROSSREF_DELETED_EVENT_URL_TEMPLATE
    url_start_date = start_date.strftime("%Y-%m-%d")
    url_end_date = end_date.strftime("%Y-%m-%d")

    event_urls = [
        event_url_template.format(doi=doi, mailto=mailto, start_date=url_start_date, end_date=url_end_date)
        for doi in dois
    ]
    edited_urls = [
        edited_event_url_template.format(doi=doi, mailto=mailto, start_date=url_start_date, end_date=url_end_date)
        for doi in dois
    ]
    deleted_urls = [
        deleted_event_url_template.format(doi=doi, mailto=mailto, start_date=url_start_date, end_date=url_end_date)
        for doi in dois
    ]
    urls = event_urls + edited_urls + deleted_urls

    all_events = []
    print(f"Beginning crossref event data download from {len(urls)} URLs with {max_threads} workers")
    print(
        f"Downloading DOI data using URL: {event_url_template.format(doi='***', mailto=mailto, start_date=url_start_date, end_date=url_end_date)}"
    )
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i, url in enumerate(urls):
            futures.append(
                executor.submit(
                    download_crossref_event_url,
                    url,
                    i=i,
                )
            )
        for future in as_completed(futures):
            all_events.extend(future.result())

    return all_events


def download_crossref_event_url(url: str, i: int = 0):
    """
    Downloads all crossref events from a url, iterating through pages if there is more than one

    :param url: The url send the request to
    :param i: Worker number
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


def download_crossref_page_events(url: str, headers: dict) -> tuple:
    """
    Download crossref events from a single page

    :param url: The url to send the request to
    :param headers: Headers to send with the request
    :return: The cursor, event counter, total numebr of events, the events
    """
    try:
        response = retry_session(num_retries=5, backoff_factor=0.8).get(url, headers=headers)
    except MaxRetryError:
        raise ConnectionError(f"Error requesting url: {url}, response: {response.text}")
    response_json = response.json()
    total_events = response_json["message"]["total-results"]
    events = response_json["message"]["events"]
    next_cursor = response_json["message"]["next-cursor"]
    counter = len(events)

    return next_cursor, counter, total_events, events


def transform_crossref_events(events: List[dict], max_threads: int = 1) -> List[dict]:
    """
    Spawns workers to transforms crossref events

    :param all_events: A list of the events to transform
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
