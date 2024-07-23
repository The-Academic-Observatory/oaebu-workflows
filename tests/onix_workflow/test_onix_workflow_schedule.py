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
from airflow.timetables.base import DataInterval
import pendulum
import time_machine

from onix_workflow_schedule import OnixWorkflowTimetable
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
    """Test the OnixWorkflowSchedule Class"""

    def test_dag_run(self):
        """Test the schedule when it's called by airflow's next_dagrun_info function"""
        env = SandboxEnvironment()
        with env.create():
            dag_start_date = pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC)
            expected_start_date = pendulum.datetime(year=2020, month=1, day=27, tz=pendulum.UTC)  # Monday
            expected_end_date = pendulum.datetime(year=2020, month=2, day=3, tz=pendulum.UTC)  # Monday
            now = pendulum.datetime(year=2020, month=2, day=2, tz=pendulum.UTC)  # Sunday
            with time_machine.travel(now):
                dag = make_test_dag(dag_start_date)
                dag_run_info = dag.next_dagrun_info(last_automated_dagrun=None)
            self.assertEqual(dag_run_info.data_interval.start, expected_start_date)
            self.assertEqual(dag_run_info.data_interval.end, expected_end_date)

            # Now run the next interval
            start_date = dag_run_info.data_interval.end
            expected_end_date = pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC)  # The 5th
            with time_machine.travel(dag_run_info.data_interval.end):
                dag = make_test_dag(dag_run_info.data_interval.end)
                dag_run_info = dag.next_dagrun_info(last_automated_dagrun=dag_run_info.data_interval)
            self.assertEqual(dag_run_info.data_interval.start, start_date)
            self.assertEqual(dag_run_info.data_interval.end, expected_end_date)

    def test_dag_run_with_catchup(self):
        """Test the schedule when it's called by airflow's next_dagrun_info function with the catchup setting on"""
        env = SandboxEnvironment()
        with env.create():
            start_date = pendulum.datetime(year=2020, month=1, day=7, tz=pendulum.UTC)  # Monday
            dag = make_test_dag(start_date, catchup=True)
            # The timetable raises a value error but it's caught by airflow. So we check that info==None
            info = dag.next_dagrun_info(last_automated_dagrun=None)
            self.assertIsNone(info)

    def test_get_start_of_interval(self):
        """Test the get_start_of_interval function"""
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),  # Friday
            pendulum.datetime(year=2020, month=10, day=5, tz=pendulum.UTC),  # Monday the 5th
            pendulum.datetime(year=2020, month=1, day=7, tz=pendulum.UTC),  # Tueday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.timezone("Etc/GMT+1")),  # Altered timezone
        ]
        expected_outputs = [
            pendulum.datetime(year=2019, month=12, day=30, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=1, day=27, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=10, day=5, tz=pendulum.UTC),  # Monday The 5th
            pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # The 5th
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # The 5th @ UTC
        ]
        for i, eo in zip(inputs, expected_outputs):
            logging.info(f"Input time: {i}")
            output = timetable.get_start_of_interval(i)
            self.assertEqual(eo, output)

    def test_get_end_of_interval(self):
        """Test the get_end_of_interval function"""
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),  # Friday
            pendulum.datetime(year=2020, month=10, day=5, tz=pendulum.UTC),  # Monday the 5th
            pendulum.datetime(year=2020, month=1, day=7, tz=pendulum.UTC),  # Tuesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.timezone("Etc/GMT+1")),  # Altered timezone
        ]
        expected_outputs = [
            pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),  # The 5th
            pendulum.datetime(year=2020, month=2, day=3, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=10, day=12, tz=pendulum.UTC),  # Monday (1 week after start)
            pendulum.datetime(year=2020, month=1, day=13, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=2, day=10, tz=pendulum.UTC),  # Monday
            pendulum.datetime(year=2020, month=2, day=10, tz=pendulum.UTC),  # Monday @ UTC
        ]
        for i, eo in zip(inputs, expected_outputs):
            logging.info(f"Input time: {i}")
            output = timetable.get_end_of_interval(i)
            self.assertEqual(eo, output)

    def test_infer_manual_data_interval(self):
        """Test the infer_manual_data_interval function"""
        timetable = OnixWorkflowTimetable()
        inputs = [
            pendulum.datetime(year=2020, month=1, day=1, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=1, day=31, tz=pendulum.UTC),  # Friday
            pendulum.datetime(year=2020, month=10, day=5, tz=pendulum.UTC),  # Monday the 5th
            pendulum.datetime(year=2020, month=1, day=7, tz=pendulum.UTC),  # Tuesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),  # Wednesday
            pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.timezone("Etc/GMT+1")),  # Altered timezone
        ]
        expected_outputs = [
            DataInterval(
                pendulum.datetime(year=2019, month=12, day=30, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=5, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=27, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=2, day=3, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=10, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=10, day=12, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=1, day=6, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=1, day=13, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=2, day=10, tz=pendulum.UTC),
            ),
            DataInterval(
                pendulum.datetime(year=2020, month=2, day=5, tz=pendulum.UTC),
                pendulum.datetime(year=2020, month=2, day=10, tz=pendulum.UTC),
            ),
        ]
        for i, eo in zip(inputs, expected_outputs):
            logging.info(f"Input interval: {i}")
            output = timetable.infer_manual_data_interval(i)
            self.assertEqual(eo, output)
