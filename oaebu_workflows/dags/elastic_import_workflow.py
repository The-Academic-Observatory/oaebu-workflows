# Copyright 2020, 2021 Curtin University
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

# Author: James Diprose

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

import json
import os
import re
from typing import Dict

from airflow.exceptions import AirflowException

from oaebu_workflows.config import elastic_mappings_folder
from observatory.platform.elastic.elastic import KeepInfo, KeepOrder
from observatory.platform.elastic.kibana import TimeField
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.workflow_utils import make_dag_id
from observatory.platform.workflows.elastic_import_workflow import (
    ElasticImportConfig,
    ElasticImportWorkflow,
)

DATASET_ID = "data_export"
DATA_LOCATION = "us"
FILE_TYPE_JSONL = "jsonl.gz"
DAG_ONIX_WORKFLOW_PREFIX = "onix_workflow"
DAG_PREFIX = "elastic_import"
ELASTIC_MAPPINGS_PATH = elastic_mappings_folder()
OAEBU_KIBANA_TIME_FIELDS = [
    TimeField("^oaebu-.*-unmatched-book-metrics$", "release_date"),
    TimeField("^oaebu-.*-book-product-list$", "time_field"),
    TimeField("^oaebu-.*$", "month"),
]

# These can be customised per DAG.  Just using some generic settings for now.
index_keep_info = {
    "": KeepInfo(ordering=KeepOrder.newest, num=3),
    "oaebu": KeepInfo(ordering=KeepOrder.newest, num=3),
}


def load_elastic_mappings_oaebu(path: str, table_prefix: str) -> Dict:
    """For the OAEBU project, load the Elastic mappings for a given table_prefix.
    :param path: the path to the mappings files.
    :param table_prefix: the table_id prefix (without shard date).
    :return: the rendered mapping as a Dict.
    """

    if not table_prefix.startswith("oaebu"):
        raise ValueError("Table must begin with 'oaebu'")
    elif "unmatched" in table_prefix:
        mappings_path = os.path.join(path, "oaebu-unmatched-metrics-mappings.json.jinja2")
        return json.loads(render_template(mappings_path))
    elif "institution_list" in table_prefix:
        mappings_path = os.path.join(path, "oaebu-institution-list-mappings.json.jinja2")
        return json.loads(render_template(mappings_path))
    else:
        # Aggregation level
        aggregation_level_search = re.search(r"(?<=book_)(.*?)(?=_)", table_prefix)
        if aggregation_level_search:
            aggregation_level = aggregation_level_search.group(1)
        else:
            raise AirflowException(f"Aggregation Level not found in table_prefix: {table_prefix}")

        # Make mappings path
        suffix = re.search(f"_book_{aggregation_level}_(.*)", table_prefix).group(1)
        mappings_file_name = f"oaebu-{suffix}-mappings.json.jinja2".replace("_", "-")
        mappings_path = os.path.join(path, mappings_file_name)

        return json.loads(render_template(mappings_path, aggregation_level=aggregation_level))


configs = [
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "anu_press"),
        project_id="oaebu-anu-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-anu-press-transform",
        elastic_conn_key="elastic_main",
        kibana_conn_key="kibana_main",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "anu_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-anu-press", "dev-oaebu-anu-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
        index_keep_info=index_keep_info,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "ucl_press"),
        project_id="oaebu-ucl-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-ucl-press-transform",
        elastic_conn_key="elastic_main",
        kibana_conn_key="kibana_main",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "ucl_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-ucl-press", "dev-oaebu-ucl-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
        index_keep_info=index_keep_info,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "wits_university_press"),
        project_id="oaebu-witts-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-witts-press-transform",
        elastic_conn_key="elastic_main",
        kibana_conn_key="kibana_main",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "wits_university_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-wits-press", "dev-oaebu-wits-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
        index_keep_info=index_keep_info,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "university_of_michigan_press"),
        project_id="oaebu-umich-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-umich-press-transform",
        elastic_conn_key="elastic_main",
        kibana_conn_key="kibana_main",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "university_of_michigan_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-umich-press", "dev-oaebu-umich-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
        index_keep_info=index_keep_info,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "university_of_michigan_press_public"),
        project_id="oaebu-umich-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-umich-press-transform",
        elastic_conn_key="elastic_public",
        kibana_conn_key="kibana_public",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "university_of_michigan_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["u-m-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
        index_keep_info=index_keep_info,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "oapen_press"),
        project_id="oaebu-oapen",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-oapen-transform",
        elastic_conn_key="elastic_main",
        kibana_conn_key="kibana_main",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=["oapen_workflow_oapen_press"],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-oapen", "dev-oaebu-oapen"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "springer_nature"),
        project_id="oaebu-springer-nature",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-springer-nature-transform",
        elastic_conn_key="elastic_main",
        kibana_conn_key="kibana_main",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "springer_nature")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-springer-nature", "dev-oaebu-springer-nature"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
    ),
]

for config in configs:
    dag = ElasticImportWorkflow(
        dag_id=config.dag_id,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        bucket_name=config.bucket_name,
        elastic_conn_key=config.elastic_conn_key,
        kibana_conn_key=config.kibana_conn_key,
        data_location=config.data_location,
        file_type=config.file_type,
        sensor_dag_ids=config.sensor_dag_ids,
        elastic_mappings_folder=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=config.elastic_mappings_func,
        kibana_spaces=config.kibana_spaces,
        kibana_time_fields=config.kibana_time_fields,
        index_keep_info=config.index_keep_info,
    ).make_dag()
    globals()[dag.dag_id] = dag
