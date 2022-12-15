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
from observatory.api.client.model.organisation import Organisation
from observatory.api.utils import get_api_client, seed_organisation


def get_organisation_info():
    organisation_info = OrderedDict()
    name = "ANU Press"
    organisation_info[name] = Organisation(
        name=name,
        project_id="oaebu-anu-press",
        download_bucket="oaebu-anu-press-download",
        transform_bucket="oaebu-anu-press-transform",
    )
    name = "UCL Press"
    organisation_info[name] = Organisation(
        name=name,
        project_id="oaebu-ucl-press",
        download_bucket="oaebu-ucl-press-download",
        transform_bucket="oaebu-ucl-press-transform",
    )
    name = "University of Michigan Press"
    organisation_info[name] = Organisation(
        name=name,
        project_id="oaebu-umich-press",
        download_bucket="oaebu-umich-press-download",
        transform_bucket="oaebu-umich-press-transform",
    )
    name = "Wits University Press"
    organisation_info[name] = Organisation(
        name=name,
        project_id="oaebu-witts-press",
        download_bucket="oaebu-witts-press-download",
        transform_bucket="oaebu-witts-press-transform",
    )
    name = "OAPEN Press"
    organisation_info[name] = Organisation(
        name=name,
        project_id="oaebu-oapen",
        download_bucket="oaebu-oapen-download",
        transform_bucket="oaebu-oapen-transform",
    )
    name = "Curtin University"
    organisation_info[name] = Organisation(
        name=name,
        project_id=None,
        download_bucket=None,
        transform_bucket=None,
    )
    return organisation_info


if __name__ == "__main__":
    api = get_api_client()
    organisation_info = get_organisation_info()
    seed_organisation(api=api, organisation_info=organisation_info)
