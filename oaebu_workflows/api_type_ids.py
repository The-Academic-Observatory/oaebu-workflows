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
# Author: Tuan Chien


class TableTypeId:
    """TableTypeId type_id constants"""

    regular = "regular"
    sharded = "sharded"
    partitioned = "partitioned"


class DatasetTypeId:
    """DatasetType type_id constants"""

    # oaebu workflows
    doab = "doab"
    oapen_metadata = "oapen_metadata"
    onix = "onix"
    google_analytics = "google_analytics"
    google_books_sales = "google_books_sales"
    google_books_traffic = "google_books_traffic"
    jstor_country = "jstor_country"
    jstor_institution = "jstor_institution"
    irus_oapen = "irus_oapen"
    ucl_discovery = "ucl_discovery"
    fulcrum = "fulcrum"

    # Workflow dataset types, i.e., dataset types for datasets created by various non Telescope workflows.
    onix_workflow = "onix_workflow"


class WorkflowTypeId:
    """WorkflowTypeId type_id constants"""

    # oaebu workflows
    doab = "doab"
    oapen_metadata = "oapen_metadata"
    onix = "onix"
    thoth_onix = "thoth_onix"
    google_analytics = "google_analytics"
    google_books = "google_books"
    jstor = "jstor"
    irus_oapen = "irus_oapen"
    ucl_discovery = "ucl_discovery"
    fulcrum = "fulcrum"

    # Workflow dataset types, i.e., dataset types for datasets created by various non Telescope workflows.
    onix_workflow = "onix_workflow"
