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
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.utils import get_api_client, get_workflow_type_ids, seed_workflow, get_organisation_ids


def get_workflow_info(api: ObservatoryApi):
    wftids = get_workflow_type_ids(api)
    orgids = get_organisation_ids(api)

    workflow_info = OrderedDict()
    name = "DOAB Telescope"
    workflow_info[name] = Workflow(
        name=name,
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.doab]),
        extra={},
        tags=None,
    )
    name = "OAPEN Metadata Workflow"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["OAPEN Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_metadata]),
        extra={},
        tags=None,
    )
    name = "OAPEN Press ONIX Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["OAPEN Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    name = "ANU Press ONIX Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    name = "UCL Press ONIX Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    name = "Wits Press ONIX Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    name = "UoMP ONIX Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    name = "OAPEN ONIX Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["OAPEN Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    name = "ANU Press Google Analytics Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.google_analytics]),
        extra={"pagepath_regex": "", "view_id": "1422597"},
        tags='["oaebu"]',
    )
    name = "ANU Press Google Books Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.google_books]),
        extra={},
        tags='["oaebu"]',
    )
    name = "UCL Press Google Books Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids["google_books"]),
        extra={},
        tags='["oaebu"]',
    )
    name = "UoMP Google Books Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.google_books]),
        extra={},
        tags='["oaebu"]',
    )
    name = "ANU Press JSTOR Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "anupress"},
        tags='["oaebu"]',
    )
    name = "UoMP JSTOR Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "universityofmichiganpress"},
        tags='["oaebu"]',
    )
    name = "UCL Press JSTOR Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "uclpress"},
        tags='["oaebu"]',
    )
    name = "Wits Press JSTOR Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "witsup"},
        tags='["oaebu"]',
    )
    name = "UCL Press OAPEN IRUS-UK Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "UCL%20Press",
            "publisher_uuid_v5": "df73bf94-b818-494c-a8dd-6775b0573bc2",
        },
        tags='["oaebu"]',
    )
    name = "ANU Press OAPEN IRUS-UK Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "ANU%20Press",
            "publisher_uuid_v5": "ddc8cc3f-dd57-40ef-b8d5-06f839686b71",
        },
        tags='["oaebu"]',
    )
    name = "UoMP OAPEN IRUS-UK Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "University%20of%20Michigan%20Press",
            "publisher_uuid_v5": "e07ce9b5-7a46-4096-8f0c-bc1920e3d889",
        },
        tags='["oaebu"]',
    )
    name = "OAPEN Press OAPEN IRUS-UK Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["OAPEN Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "",
            "publisher_uuid_v5": "",
        },
        tags='["oaebu"]',
    )
    name = "Wits Press OAPEN IRUS-UK Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids["oapen_irus_uk"]),
        extra={
            "publisher_name_v4": "Wits%20University%20Press",
            "publisher_uuid_v5": "c522c2dd-daf5-4926-bf1a-ee1557d24a4b",
        },
        tags='["oaebu"]',
    )
    name = "UCL Press UCL Discovery Telescope"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.ucl_discovery]),
        extra={},
        tags='["oaebu"]',
    )
    name = "ANU Press ONIX Workflow"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags=None,
    )
    name = "UCL Press ONIX Workflow"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags=None,
    )
    name = "Wits Press ONIX Workflow"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags=None,
    )
    name = "UoMP ONIX Workflow"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags=None,
    )
    name = "OAPEN Press ONIX Workflow"
    workflow_info[name] = Workflow(
        name=name,
        organisation=Organisation(id=orgids["OAPEN Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags=None,
    )
    return workflow_info


if __name__ == "__main__":
    api = get_api_client()
    workflow_info = get_workflow_info(api)
    seed_workflow(api=api, workflow_info=workflow_info)
