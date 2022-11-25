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


from collections import OrderedDict

from oaebu_workflows.api_type_ids import WorkflowTypeId
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.utils import get_api_client, seed_workflow_type


def get_workflow_type_info():
    workflow_type_info = OrderedDict()
    workflow_type_info[WorkflowTypeId.doab] = WorkflowType(
        type_id=WorkflowTypeId.doab,
        name="DOAB Telescope",
    )
    workflow_type_info[WorkflowTypeId.google_analytics] = WorkflowType(
        type_id=WorkflowTypeId.google_analytics,
        name="Google Analytics Telescope",
    )
    workflow_type_info[WorkflowTypeId.google_books] = WorkflowType(
        type_id=WorkflowTypeId.google_books,
        name="Google Books Telescope",
    )
    workflow_type_info[WorkflowTypeId.jstor] = WorkflowType(
        type_id=WorkflowTypeId.jstor,
        name="JSTOR Telescope",
    )
    workflow_type_info[WorkflowTypeId.oapen_irus_uk] = WorkflowType(
        type_id=WorkflowTypeId.oapen_irus_uk,
        name="OAPEN IRUS-UK Telescope",
    )
    workflow_type_info[WorkflowTypeId.oapen_metadata] = WorkflowType(
        type_id=WorkflowTypeId.oapen_metadata,
        name="OAPEN Metadata Workflow",
    )
    workflow_type_info[WorkflowTypeId.onix] = WorkflowType(
        type_id=WorkflowTypeId.onix,
        name="ONIX Telescope",
    )
    workflow_type_info[WorkflowTypeId.onix_workflow] = WorkflowType(
        type_id=WorkflowTypeId.onix_workflow,
        name="ONIX Workflow",
    )
    workflow_type_info[WorkflowTypeId.ucl_discovery] = WorkflowType(
        type_id=WorkflowTypeId.ucl_discovery,
        name="UCL Discovery Telescope",
    )
    return workflow_type_info


if __name__ == "__main__":
    api = get_api_client()
    workflow_type_info = get_workflow_type_info()
    seed_workflow_type(api=api, workflow_type_info=workflow_type_info)
