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
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.utils import get_api_client, seed_workflow_type


def get_workflow_type_info():
    workflow_type_info = OrderedDict()
    workflow_type_info["doab"] = WorkflowType(type_id="doab", name="DOAB")
    workflow_type_info["google_analytics"] = WorkflowType(type_id="google_analytics", name="Google Analytics")
    workflow_type_info["google_books"] = WorkflowType(type_id="google_books", name="Google Books")
    workflow_type_info["jstor"] = WorkflowType(type_id="jstor", name="JSTOR")
    workflow_type_info["oapen_irus_uk"] = WorkflowType(type_id="oapen_irus_uk", name="OAPEN IRUS UK")
    workflow_type_info["oapen_metadata"] = WorkflowType(type_id="oapen_metadata", name="OAPEN Metadata")
    workflow_type_info["oapen_workflow"] = WorkflowType(type_id="oapen_workflow", name="OAPEN Workflow")
    workflow_type_info["onix"] = WorkflowType(type_id="onix", name="ONIX")
    workflow_type_info["onix_workflow"] = WorkflowType(type_id="onix_workflow", name="ONIX Workflow")
    workflow_type_info["ucl_discovery"] = WorkflowType(type_id="ucl_discovery", name="UCL Discovery")
    return workflow_type_info


if __name__ == "__main__":
    api = get_api_client()
    workflow_type_info = get_workflow_type_info()
    seed_workflow_type(api=api, workflow_type_info=workflow_type_info)
