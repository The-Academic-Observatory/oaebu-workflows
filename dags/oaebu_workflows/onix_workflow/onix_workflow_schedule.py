from pendulum import Date, DateTime, Time, datetime, timedelta, UTC

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

# DOCS: https://airflow.apache.org/docs/apache-airflow/stable/howto/timetable.html#define-scheduling-logic


class OnixWorkflowTimetable(Timetable):

    def get_end_of_interval(self, start_time: DateTime) -> DateTime:
        """Find the end time given a start time

        :param start_time: The starting datetime for which to find the ending interval for
        :return: The end of the interval
        """
        provisional_end = DateTime.combine((start_time + timedelta(days=7)).date(), Time.min)
        competing_end = datetime(year=provisional_end.year, month=provisional_end.month, day=5)

        # If the start date is before the 5th of the "end" month, cut the end date off at the 5th
        if start_time < competing_end:
            end_time = competing_end
        else:
            end_time = provisional_end

        return end_time

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """Overrides the base class function.
        When a DAG run is manually triggered, infer a data interval for it.

        :param run_after: The time that the run is triggered.
        """

        # Start of interval - the end of the previous day
        start_date = (run_after - timedelta(days=1)).date()
        start = DateTime.combine(start_date, Time.min).replace(tzinfo=UTC)
        end = self.get_end_of_interval(start)

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
            next_start = max(restriction.earliest, DateTime.combine(Date.today(), Time.min))

        return DagRunInfo.interval(start=next_start, end=self.get_end_of_interval(next_start))


class OnixWorkflowTimetablePlugin(AirflowPlugin):
    # I don't know why, but the documentation says this is required
    name = "onix_workflow_timetable_plugin"
    timetables = [OnixWorkflowTimetable]
