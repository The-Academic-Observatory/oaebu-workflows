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

# Author: Keegan Smith

from airflow.api.common.experimental.pool import create_pool, get_pool
from airflow.exceptions import PoolNotFound


class AirflowPool:
    def __init__(self, pool_name: str, pool_slots: int, pool_description: str):
        """Constructor an AirflowPool instance

        :param pool_name: The name of this pool
        :param pool_slots: The number of slots assigned to this pool
        :param pool_description: A description of this pool
        """
        self.pool_name = pool_name
        self.pool_slots = pool_slots
        self.pool_description = pool_description

    def get_pool(self):
        return get_pool(self.pool_name)

    def create_pool(self):
        return create_pool(self.pool_name, self.pool_slots, self.pool_description)

    def create_or_get_pool(self):
        try:
            return self.get_pool()
        except PoolNotFound:
            return self.create_pool()


class CrossrefEventsPool(AirflowPool):
    def __init__(self, pool_slots: int = 15):
        """Constructor CrossrefEventsPool instance

        :param pool_slots: The number of slots assigned to this pool
        """
        super().__init__(
            pool_name="Crossref Events Pool", pool_slots=pool_slots, pool_description="Crossref Events API Pool"
        )
