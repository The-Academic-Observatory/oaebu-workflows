from datetime import timedelta

from pendulum import Date, DateTime, Time, datetime, UTC

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

# DOCS: https://airflow.apache.org/docs/apache-airflow/stable/howto/timetable.html#define-scheduling-logic


class OnixWorkflowTimetable(Timetable):
    """A custom timetable for the Onix Workflow. The timetable runs every sunday and on the 5th of every month

    *Known Quirks*
    This timetable is a plugin and must be registered with airflow to be used. If it's not registered, it will raise
    an exception. At the time of writing, airflow has specific requirements for timetable plugin imports. THEY MUST
    BE IMPORTED RELATIVE TO THE PLUGIN FOLDER.
    https://github.com/apache/airflow/discussions/23758
    """

    def get_start_of_interval(self, time: DateTime) -> DateTime:
        """Gets the start of the interval for the schedule, given a current datetime

        :param time: The time with which to calculate the previous runtime for
        :return: The previous runtime
        """

        # Get the previous monday
        start_time = time - timedelta(time.weekday())

        # Don't allow the start date to cross the 5th of the month
        if time >= time.replace(day=5) and start_time <= time.replace(day=5):
            start_time = datetime(year=start_time.year, month=start_time.month, day=5)
        return DateTime.combine(start_time, Time.min).replace(tzinfo=UTC)

    def get_end_of_interval(self, time: DateTime) -> DateTime:
        """Find the end time given a start time

        :param time: The starting datetime for which to find the ending interval for
        :return: The end of the interval
        """

        # Get the next monday
        days_delta = 7 - (time.weekday())
        end_time = time + timedelta(days_delta)

        # Don't allow the end date to cross the 5th of the month
        if time < time.replace(day=5) and end_time > time.replace(day=5):
            end_time = end_time.replace(day=5)
        return DateTime.combine(end_time, Time.min).replace(tzinfo=UTC)

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """Overrides the base class function.
        When a DAG run is manually triggered, infer a data interval for it.

        :param run_after: The time that the run is triggered.
        """

        # Start of interval - the end of the previous sunday or the 5th
        start = self.get_start_of_interval(run_after)
        end = self.get_end_of_interval(run_after)

        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo:
        """Overrides the base class function
        Provide information to schedule the next DagRun.

        :param last_automated_data_interval: The data interval of the associated
            DAG's last scheduled or backfilled run (manual runs not considered).
        :param restriction: Restriction to apply when scheduling the DAG run.
            See documentation of :class:`TimeRestriction` for details.
        :return: Information on when the next DagRun can be scheduled.
        """

        if restriction.catchup:
            raise ValueError("Onix Workflow timetable received unexpected catchup=True setting")

        # There was a previous run on the regular schedule.
        if last_automated_data_interval is not None:
            last_start = last_automated_data_interval.start
            next_start = self.get_end_of_interval(last_start)

        # Otherwise this is the first ever run on the regular schedule...
        else:
            # Make sure the interval start is a sunday
            after_start_date = self.get_start_of_interval(Date.today())
            next_start = max(restriction.earliest, after_start_date)

        return DagRunInfo.interval(start=next_start, end=self.get_end_of_interval(next_start))


class OnixWorkflowTimetablePlugin(AirflowPlugin):
    name = "onix_workflow_timetable_plugin"
    timetables = [OnixWorkflowTimetable]
