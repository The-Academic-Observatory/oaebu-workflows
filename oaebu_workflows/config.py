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

# Author: James Diprose

import json
import os
import re
from typing import Dict

from airflow.exceptions import AirflowException

from observatory.platform.config import module_file_path
from observatory.platform.utils.jinja2_utils import render_template


def test_fixtures_folder(*subdirs) -> str:
    """Get the path to the Academic Observatory Workflows test data directory.

    :return: the test data directory.
    """

    base_path = module_file_path("oaebu_workflows.fixtures")
    return os.path.join(base_path, *subdirs)


def schema_folder() -> str:
    """Return the path to the database schema template folder.

    :return: the path.
    """

    return module_file_path("oaebu_workflows.database.schema")


def sql_folder() -> str:
    """Return the path to the workflow SQL template folder.

    :return: the path.
    """

    return module_file_path("oaebu_workflows.database.sql")


####################################
### For Elastic workflow Imports ###
####################################


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


def elastic_mappings_folder() -> str:
    """Get the Elasticsearch mappings path.

    :return: the elastic search schema path.
    """

    return module_file_path("oaebu_workflows.database.mappings")
