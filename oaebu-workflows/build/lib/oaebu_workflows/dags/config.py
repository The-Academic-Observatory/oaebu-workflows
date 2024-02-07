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

# Author: James Diprose, Keegan Smith

import os
from typing import Optional

from observatory.platform.config import module_file_path


def test_fixtures_folder(workflow_module: Optional[str] = None) -> str:
    """Get the path to the OAeBU Workflows test data directory.

    :param workflow_module: Optional, name of the workflow. Only to be included if the schema for the workflow is in
    the directory oaebu_workflows.{workflow_name}.tests.fixtures
    :return: the test fixtures directory.
    """

    if workflow_module is not None:
        return construct_module_path("oaebu_workflows", workflow_module, "tests", "fixtures")

    return construct_module_path("oaebu_workflows", "tests", "fixtures")


def schema_folder(workflow_module: Optional[str] = None) -> str:
    """Return the path to the schema template folder.

    :param workflow_module: Optional, name of the workflow. Only to be included if the schema for the workflow is in
    the directory oaebu_workflows.{workflow_module}.schema
    :return: the path.
    """

    if workflow_module is not None:
        return construct_module_path("oaebu_workflows", workflow_module, "schema")

    return construct_module_path("oaebu_workflows", "schema")


def sql_folder(workflow_module: Optional[str] = None) -> str:
    """Return the path to the workflow SQL template folder.

    :param workflow_module: Optional, name of the workflow. Only to be included if the sql for the workflow is in
    the directory oaebu_workflows.{workflow_module}.sql
    :return: the path.
    """

    if workflow_module is not None:
        return construct_module_path("oaebu_workflows", workflow_module, "sql")

    return construct_module_path("oaebu_workflows", "sql")


def construct_module_path(*parts: str) -> str:
    """Constructs the full module path given parts of a path."""

    module_path = ".".join(list(parts))
    file_path = module_file_path(module_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"construct_module_path: directory {file_path} does not exist!")

    return file_path
