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

import logging
import unittest

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
import pendulum

from oaebu_workflows.onix_workflow.onix_workflow_schedule import OnixWorkflowTimetable
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment


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
    def test_dag_run(self):
        env = SandboxEnvironment()
        with env.create():
            start_date = pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC)  # Sunday
            dag = make_test_dag(start_date)
            dag_run_info = dag.next_dagrun_info(last_automated_dagrun=None)
            self.assertEqual(dag_run_info.data_interval.start, start_date)
            self.assertEqual(dag_run_info.data_interval.end, start_date + pendulum.timedelta(days=7))

    def test_dag_run_with_catchup(self):
        env = SandboxEnvironment()
        with env.create():
            start_date = pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC)  # Sunday
            dag = make_test_dag(start_date, catchup=True)
            with self.assertRaisesRegex(ValueError, "Onix Workflow timetable"):
                dag.next_dagrun_info(last_automated_dagrun=None)

    def test_get_start_of_interval(self):
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),  # Friday
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),  # Sunday the 5th
            pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.timezone("Etc/GMT+1")),  # Altered timezone
        ]
        expected_outputs = [
            pendulum.datetime(year=2019, month=12, day=29, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=26, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
        ]
        for i, eo in zip(inputs, expected_outputs):
            logging.info(f"Input time: {i}")
            output = timetable.get_start_of_interval(i)
            self.assertEqual(eo, output)

    def test_get_end_of_interval(self):
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),  # Friday
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),  # Sunday the 5th
            pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.timezone("Etc/GMT+1")),  # Altered timezone
        ]
        expected_outputs = [
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=2, day=2, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=12, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=1, day=12, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=2, day=9, tz=pendulum.UTC),
            pendulum.datetime(year=2020, month=2, day=9, tz=pendulum.UTC),
        ]
        for i, eo in zip(inputs, expected_outputs):
            logging.info(f"Input time: {i}")
            output = timetable.get_end_of_interval(i)
            self.assertEqual(eo, output)

    def test_infer_manual_data_interval(self):
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),  # Friday
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),  # Sunday the 5th
            pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.timezone("Etc/GMT+1")),  # Altered timezone
        ]
        expected_outputs = [
            DataInterval(
                pendulum.datetime(year=2019, month=12, day=29, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=26, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=2, day=2, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=12, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=12, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=2, day=9, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=2, day=9, tz=pendulum.UTC),
            ),
        ]
        for i, eo in zip(inputs, expected_outputs):
            output = timetable.infer_manual_data_interval(i)
            self.assertEqual(eo, output)
