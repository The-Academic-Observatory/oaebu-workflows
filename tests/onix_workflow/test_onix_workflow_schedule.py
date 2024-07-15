# Copyright 2020-2024 Curtin University
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

import unittest

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
import pendulum

from oaebu_workflows.onix_workflow.onix_workflow_schedule import OnixWorkflowTimetable
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction


def make_test_dag(start_date=pendulum.datetime(year=2020, month=1, day=1), catchup=False):
    @dag(
        dag_id="test_dag",
        schedule=OnixWorkflowTimetable(),
        start_date=start_date,
        catchup=catchup,
    )
    def my_dag():
        EmptyOperator(task_id="test_task")

    return my_dag()


class TestOnixWorkflowSchedule(unittest.TestCase):
    def test_get_end_of_interval(self):
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),
        ]
        expected_outputs = [
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=12, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=13, tz=pendulum.UTC),
        ]
        for i, eo in zip(inputs, expected_outputs):
            output = timetable.get_end_of_interval(i)
            self.assertEqual(output, eo)

    def test_infer_manual_data_interval(self):
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),
        ]
        expected_outputs = [
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=12, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=13, tz=pendulum.UTC),
            ),
        ]
        for i, eo in zip(inputs, expected_outputs):
            output = timetable.infer_manual_data_interval(i)
            self.assertEqual(output, eo)
