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
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.organisation import Organisation
from observatory.api.utils import get_api_client, get_workflow_type_ids, seed_workflow, get_organisation_ids
from observatory.api.client.api.observatory_api import ObservatoryApi
from oaebu_workflows.api_type_ids import WorkflowTypeId


def get_workflow_info(api: ObservatoryApi):
    wftids = get_workflow_type_ids(api)
    orgids = get_organisation_ids(api)

    workflow_info = OrderedDict()
    workflow_info["DOAB Telescope"] = Workflow(
        name="DOAB Telescope",
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.doab]),
        extra={},
        tags=None,
    )
    workflow_info["OAPEN Metadata Telescope"] = Workflow(
        name="OAPEN Metadata Telescope",
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_metadata]),
        extra={},
        tags=None,
    )
    workflow_info["ANU Press ONIX Telescope"] = Workflow(
        name="ANU Press ONIX Telescope",
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    workflow_info["UCL Press ONIX Telescope"] = Workflow(
        name="UCL Press ONIX Telescope",
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    workflow_info["Witts University Press ONIX Telescope"] = Workflow(
        name="Witts University Press ONIX Telescope",
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    workflow_info["UoMP ONIX Telescope"] = Workflow(
        name="UoMP ONIX Telescope",
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix]),
        extra={"date_format": "%Y%m%d", "date_regex": "\\d{8}"},
        tags=None,
    )
    workflow_info["ANU Press Google Analytics Telescope"] = Workflow(
        name="ANU Press Google Analytics Telescope",
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.google_analytics]),
        extra={"pagepath_regex": "", "view_id": "1422597"},
        tags='["oaebu"]',
    )
    workflow_info["ANU Press Google Books Telescope"] = Workflow(
        name="ANU Press Google Books Telescope",
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.google_books]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["UCL Press Google Books Telescope"] = Workflow(
        name="UCL Press Google Books Telescope",
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids["google_books"]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["UoMP Google Books Telescope"] = Workflow(
        name="UoMP Google Books Telescope",
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.google_books]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["ANU Press JSTOR Telescope"] = Workflow(
        name="ANU Press JSTOR Telescope",
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "anupress"},
        tags='["oaebu"]',
    )
    workflow_info["UoMP JSTOR Telescope"] = Workflow(
        name="UoMP JSTOR Telescope",
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "universityofmichiganpress"},
        tags='["oaebu"]',
    )
    workflow_info["UCL Press JSTOR Telescope"] = Workflow(
        name="UCL Press JSTOR Telescope",
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "uclpress"},
        tags='["oaebu"]',
    )
    workflow_info["Wits Press JSTOR Telescope"] = Workflow(
        name="Wits Press JSTOR Telescope",
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.jstor]),
        extra={"publisher_id": "witsup"},
        tags='["oaebu"]',
    )
    workflow_info["UCL Press OAPEN IRUS-UK Telescope"] = Workflow(
        name="UCL Press OAPEN IRUS-UK Telescope",
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "UCL%20Press",
            "publisher_uuid_v5": "df73bf94-b818-494c-a8dd-6775b0573bc2",
        },
        tags='["oaebu"]',
    )
    workflow_info["ANU Press OAPEN IRUS-UK Telescope"] = Workflow(
        name="ANU Press OAPEN IRUS-UK Telescope",
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "ANU%20Press",
            "publisher_uuid_v5": "ddc8cc3f-dd57-40ef-b8d5-06f839686b71",
        },
        tags='["oaebu"]',
    )
    workflow_info["UoMP OAPEN IRUS-UK Telescope"] = Workflow(
        name="UoMP OAPEN IRUS-UK Telescope",
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "University%20of%20Michigan%20Press",
            "publisher_uuid_v5": "e07ce9b5-7a46-4096-8f0c-bc1920e3d889",
        },
        tags='["oaebu"]',
    )
    workflow_info["OAPEN Press OAPEN IRUS-UK Telescope"] = Workflow(
        name="OAPEN Press OAPEN IRUS-UK Telescope",
        organisation=Organisation(id=orgids["OAPEN Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.oapen_irus_uk]),
        extra={
            "publisher_name_v4": "",
            "publisher_uuid_v5": "",
        },
        tags='["oaebu"]',
    )
    workflow_info["Wits Press OAPEN IRUS-UK Telescope"] = Workflow(
        name="Wits Press OAPEN IRUS-UK Telescope",
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids["oapen_irus_uk"]),
        extra={
            "publisher_name_v4": "Wits%20University%20Press",
            "publisher_uuid_v5": "c522c2dd-daf5-4926-bf1a-ee1557d24a4b",
        },
        tags='["oaebu"]',
    )
    workflow_info["UCL Press UCL Discovery Telescope"] = Workflow(
        name="UCL Press UCL Discovery Telescope",
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.ucl_discovery]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["ANU Press ONIX Workflow"] = Workflow(
        name="ANU Press ONIX Workflow",
        organisation=Organisation(id=orgids["ANU Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["UCL Press ONIX Workflow"] = Workflow(
        name="UCL Press ONIX Workflow",
        organisation=Organisation(id=orgids["UCL Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["Wits Press ONIX Workflow"] = Workflow(
        name="Wits Press ONIX Workflow",
        organisation=Organisation(id=orgids["Wits University Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["UoMP ONIX Workflow"] = Workflow(
        name="UoMP ONIX Workflow",
        organisation=Organisation(id=orgids["University of Michigan Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags='["oaebu"]',
    )
    workflow_info["OAPEN Workflow"] = Workflow(
        name="OAPEN Workflow",
        organisation=Organisation(id=orgids["OAPEN Press"]),
        workflow_type=WorkflowType(id=wftids[WorkflowTypeId.onix_workflow]),
        extra={},
        tags='["oaebu"]',
    )
    return workflow_info


if __name__ == "__main__":
    api = get_api_client()
    workflow_info = get_workflow_info(api)
    seed_workflow(api=api, workflow_info=workflow_info)
