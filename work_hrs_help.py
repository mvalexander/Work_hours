#######################################################
# work_hrs_help.py - A helper module for work_hours.py
# written by Mark Alexander (alexander.markv@gmail.com)
#######################################################
import sqlite3
import pandas as pd
import datetime as dt
import re
import sys
from loguru import logger

DATE_TIME_FMT_STR = "%Y-%m-%d %H:%M"
DATE_FMT_STR = "%Y-%m-%d"


def read_work_hrs_table(db_table):
    """
    Read a work hours table from SQLite database and create a DataFrame

    :param db_table: name of the work hours table
    :return: Pandas DataFrame of info from the work hours table
    """
    try:
        with sqlite3.connect("work_hours.sqlite") as conn:
            select_str = f"SELECT * FROM {db_table}"
            return pd.read_sql(select_str, conn, index_col="id")
    except FileNotFoundError:
        logger.critical("SQL file not found! Exiting.")
        sys.exit()


def write_work_hrs_table(db_table, work_hrs_date_df, new_work_hrs_date_df):
    """
    Write new shift info to a work hours table in SQLite database

    :param db_table: name of the database work hours table
    :param work_hrs_date_df: original work hours DataFrame before modification
    :param new_work_hrs_date_df: DataFrame with new work hours information
    :return: None
    """
    try:
        with sqlite3.connect("work_hours.sqlite") as conn:
            cursor = conn.cursor()
            if work_hrs_date_df.shape[0] == new_work_hrs_date_df.shape[0]:
                logger.info("Updating row in {}.", db_table)
                for idx, row in new_work_hrs_date_df.iterrows():
                    update_str = f'UPDATE {db_table} SET start="{row.start}", end="{row.end}", scheduled={row.scheduled} WHERE start="{work_hrs_date_df.iloc[idx].start}"'
                    cursor.execute(update_str)
            else:
                if work_hrs_date_df.empty:
                    logger.info("Inserting row into {}.", db_table)
                    for idx, row in new_work_hrs_date_df.iterrows():
                        add_str = f'INSERT INTO {db_table} (date, start, end, scheduled) VALUES ("{new_work_hrs_date_df.iloc[idx].date}", "{new_work_hrs_date_df.iloc[idx].start}", "{new_work_hrs_date_df.iloc[idx].end}", {new_work_hrs_date_df.iloc[idx].scheduled})'
                        cursor.execute(add_str)
                else:
                    logger.info("Deleting row and inserting new row in {}.", db_table)
                    for idx, row in work_hrs_date_df.iterrows():
                        delete_str = f'DELETE FROM {db_table} WHERE start="{work_hrs_date_df.iloc[idx].start}"'
                        cursor.execute(delete_str)
                    for idx, row in new_work_hrs_date_df.iterrows():
                        add_str = f'INSERT INTO {db_table} (date, start, end, scheduled) VALUES ("{new_work_hrs_date_df.iloc[idx].date}", "{new_work_hrs_date_df.iloc[idx].start}", "{new_work_hrs_date_df.iloc[idx].end}", {new_work_hrs_date_df.iloc[idx].scheduled})'
                        cursor.execute(add_str)

            conn.commit()
            cursor.close()
    except FileNotFoundError:
        logger.critical("SQL file not found! Exiting.")
        sys.exit()


def compute_delta_hrs_min(delta):
    hrs, remainder = divmod(delta.seconds, 3600)
    hrs += delta.days * 24
    minutes, seconds = divmod(remainder, 60)
    return hrs, minutes


def compute_daily_hrs(df_table, dt_object, tdelta_as_hrs_min=False):
    """Daily hours computation

    Takes a table (DataFrame) of work hours and a date and returns the hours worked.

    :param df_table: a Pandas DataFrame of shifts worked for a single job
    :param dt_object: a datetime object with the date of interest
    :param tdelta_as_hrs_min: boolean that determines if a datetime object (True) or HH:MM string is returned (False)

    :return: dict with a list of shifts, a list of total hours for those shifts, and a total of hours worked this job on this date
        - depending on the tdelta_as_hrs_min boolean, shift total and day total will be a datetime.tdelta obj or HH:MM string
    """
    shifts = []
    shift_deltas = []
    scheduled = []

    date_str = dt_object.strftime(DATE_FMT_STR)
    try:
        df_date = df_table[df_table.date == date_str]
        if df_date.empty:
            return None
    except KeyError as e:
        logger.critical("KeyError: {} not found", e)
        return None

    for index, row in df_date.iterrows():
        shifts.append(row.start.split()[1] + "-" + row.end.split()[1])
        shift_delta = dt.datetime.strptime(
            row.end, DATE_TIME_FMT_STR
        ) - dt.datetime.strptime(row.start, DATE_TIME_FMT_STR)
        shift_deltas.append(shift_delta)
        try:
            scheduled.append(row.scheduled)
        except AttributeError:
            scheduled.append(False)

    shifts_tot = pd.Series(shift_deltas).sum()

    if tdelta_as_hrs_min:
        for idx, item in enumerate(shift_deltas):
            hrs, mins = compute_delta_hrs_min(item)
            shift_deltas[idx] = f"{hrs:02}:{mins:02}"
        hrs, mins = compute_delta_hrs_min(shifts_tot)
        shifts_tot = f"{hrs:02}:{mins:02}"

    return {
        "shifts": shifts,
        "shift_deltas": shift_deltas,
        "shifts_tot": shifts_tot,
        "scheduled": scheduled,
    }


def display_alerts(eight_day_df):
    """Display alerts, warnings, notes based on DataFrame of hours worked

    :param eight_day_df: DataFrame with hours worked

    :return: string of alerts, warnings, and errors
    """
    ALERT_80_HRS = f"{'-ALERT-:':<10}Exceeding 80 hours on"
    WARN_80_HRS = f"{'Warning:':<10}Exceeding 75 of 80 hours on"
    NOTE_80_HRS = f"{'Note:':<10}Exceeding 70 of 80 hours on"
    ALERT_15_WORK_HRS = f"{'-ALERT-:':<10}Exceeding 15 working hours on"
    WARN_15_WORK_HRS = f"{'Warning:':<10}Exceeding 12 of 15 working hours on"
    NOTE_15_WORK_HRS = f"{'Note:':<10}Exceeding 10 of 15 working hours on"
    ALERT_12_DRIVE_HRS = f"{'-ALERT-:':<10}Exceeding 12 driving hours on"
    WARN_12_DRIVE_HRS = f"{'Warning:':<10}Exceeding 10 of 12 driving hours on"
    NOTE_12_DRIVE_HRS = f"{'Note:':<10}Exceeding 8 of 12 driving hours on"

    DELTA_80_HRS = dt.timedelta(days=3, hours=8)
    DELTA_75_HRS = dt.timedelta(days=3, hours=3)
    DELTA_70_HRS = dt.timedelta(days=2, hours=22)
    DELTA_15_HRS = dt.timedelta(days=0, hours=15)
    DELTA_12_HRS = dt.timedelta(days=0, hours=12)
    DELTA_10_HRS = dt.timedelta(days=0, hours=10)
    DELTA_8_HRS = dt.timedelta(days=0, hours=8)

    new_notifications = []

    # All days which exceed 70 hours in rolling window
    eighty_in_8_df = eight_day_df[eight_day_df.eight_day_window >= DELTA_70_HRS]

    # ALERT for >= 80
    for row in eighty_in_8_df.iterrows():
        if row[1].eight_day_window >= DELTA_80_HRS:
            new_notifications.append(f"{ALERT_80_HRS} {row[1].date}\n")

    # WARN for >= 75
    eighty_in_8_df = eighty_in_8_df[eight_day_df.eight_day_window < DELTA_80_HRS]
    for row in eighty_in_8_df.iterrows():
        if row[1].eight_day_window > DELTA_75_HRS:
            new_notifications.append(f"{WARN_80_HRS} {row[1].date}\n")

    # NOTE for >= 70
    eighty_in_8_df = eighty_in_8_df[eight_day_df.eight_day_window < DELTA_75_HRS]
    for row in eighty_in_8_df.iterrows():
        if row[1].eight_day_window > DELTA_70_HRS:
            new_notifications.append(f"{NOTE_80_HRS} {row[1].date}\n")

    # All days which exceed 10 hours in daily total window
    fifteen_working_df = eight_day_df[eight_day_df.daily_tot_hrs >= DELTA_10_HRS]

    # ALERT for >= 15
    for row in fifteen_working_df.iterrows():
        if row[1].daily_tot_hrs >= DELTA_15_HRS:
            new_notifications.append(f"{ALERT_15_WORK_HRS} {row[1].date}\n")

    # ALERT for >= 12
    fifteen_working_df = fifteen_working_df[eight_day_df.daily_tot_hrs < DELTA_15_HRS]
    for row in fifteen_working_df.iterrows():
        if row[1].daily_tot_hrs > DELTA_12_HRS:
            new_notifications.append(f"{WARN_15_WORK_HRS} {row[1].date}\n")

    # ALERT for >= 12
    fifteen_working_df = fifteen_working_df[eight_day_df.daily_tot_hrs < DELTA_12_HRS]
    for row in fifteen_working_df.iterrows():
        if row[1].daily_tot_hrs > DELTA_10_HRS:
            new_notifications.append(f"{NOTE_15_WORK_HRS} {row[1].date}\n")

    # All days which exceed 12 hours in driving total window
    twelve_driving_df = eight_day_df[eight_day_df.drive_tot_hrs >= DELTA_8_HRS]

    # ALERT for >= 12
    for row in twelve_driving_df.iterrows():
        if row[1].drive_tot_hrs >= DELTA_12_HRS:
            new_notifications.append(f"{ALERT_12_DRIVE_HRS} {row[1].date}\n")

    # ALERT for >= 10
    twelve_driving_df = twelve_driving_df[eight_day_df.drive_tot_hrs < DELTA_12_HRS]
    for row in twelve_driving_df.iterrows():
        if row[1].drive_tot_hrs > DELTA_10_HRS:
            new_notifications.append(f"{WARN_12_DRIVE_HRS} {row[1].date}\n")

    # ALERT for >= 8
    twelve_driving_df = twelve_driving_df[eight_day_df.drive_tot_hrs < DELTA_10_HRS]
    for row in twelve_driving_df.iterrows():
        if row[1].drive_tot_hrs > DELTA_8_HRS:
            new_notifications.append(f"{NOTE_12_DRIVE_HRS} {row[1].date}\n")

    return "".join(new_notifications)


def compute_eight_day_df(bus_hrs_df, HD_hrs_df, delivery_hrs_df):
    """Compute a DataFrame that combines all DataFrames of work hours, creating a new table that sums work hours for each shift
      for each job, a daily total for all jobs, and an eight day rolling sum

    :param bus_hrs_df: DataFrame of work hours for bus
    :param HD_hrs_df: DataFrame of Home Depot hours for bus
    :param delivery_hrs_df: DataFrame of delivery hours for bus

    :return: a DataFrame containing daily sums of work hours for each job, a daily total of work hours, and an 8 day rolling sum
    """
    min_date = min(
        bus_hrs_df.date.min(), HD_hrs_df.date.min(), delivery_hrs_df.date.min()
    )
    max_date = max(
        bus_hrs_df.date.max(), HD_hrs_df.date.max(), delivery_hrs_df.date.max()
    )

    eight_day_df = pd.DataFrame(
        columns=[
            "date",
            "bus_tot_hrs",
            "HD_tot_hrs",
            "deliver_tot_hrs",
            "daily_tot_hrs",
            "eight_day_window",
            "drive_tot_hrs",
        ]
    )
    hrs_columns = ["bus_tot_hrs", "HD_tot_hrs", "deliver_tot_hrs"]
    date_range = pd.date_range(start=min_date, end=max_date)
    row_data_dict = {
        "date": 0,
        "bus_tot_hrs": dt.timedelta(),
        "HD_tot_hrs": dt.timedelta(),
        "deliver_tot_hrs": dt.timedelta(),
        "daily_tot_hrs": dt.timedelta(),
        "eight_day_window": 0,
    }
    for idx_date in date_range:
        row_data_dict["date"] = idx_date.strftime(DATE_FMT_STR)
        for hrs_idx, item_dict in zip(
            hrs_columns,
            [
                compute_daily_hrs(bus_hrs_df, idx_date, tdelta_as_hrs_min=False),
                compute_daily_hrs(HD_hrs_df, idx_date, tdelta_as_hrs_min=False),
                compute_daily_hrs(delivery_hrs_df, idx_date, tdelta_as_hrs_min=False),
            ],
        ):
            if item_dict is None:
                row_data_dict[hrs_idx] = dt.timedelta()
            else:
                row_data_dict[hrs_idx] = item_dict["shifts_tot"]
        row_data_dict["daily_tot_hrs"] = (
            row_data_dict["bus_tot_hrs"]
            + row_data_dict["HD_tot_hrs"]
            + row_data_dict["deliver_tot_hrs"]
        )
        row_data_dict["eight_day_window"] = row_data_dict["daily_tot_hrs"]
        row_data_dict["drive_tot_hrs"] = (
            row_data_dict["bus_tot_hrs"] + row_data_dict["deliver_tot_hrs"]
        )

        eight_day_df = eight_day_df.append(row_data_dict, ignore_index=True)

    window = [dt.timedelta()] * 8
    rolling_window_series = eight_day_df.eight_day_window.copy()
    for idx, item in enumerate(rolling_window_series):
        window.append(item)
        window.pop(0)
        rolling_window_series[idx] = sum(window, dt.timedelta())
    eight_day_df["eight_day_window"] = rolling_window_series

    return eight_day_df


class Week:
    def __init__(self, work_hrs_df, dt_day_object):
        """
        A Week class which contains all days of the week. Dates and work info for a given table will be computed
          starting with Monday of the week given by dt_day_object.

        :param work_hrs_df: The work hours DataFrame for a job
        :param dt_day_object: Any date that falls in this week of interest
        """
        # given a date, fill out the Mon-Sun week with correct dates
        self._week_dates_list = []
        for idx in range(7):
            self._week_dates_list.append(
                dt_day_object + dt.timedelta(days=(idx - dt_day_object.weekday()))
            )
        self._shifts = []
        self._shift_deltas = []
        self._shifts_tot = []
        self._scheduled = []
        # fill in information for each day with info from the DataFrame
        for idx in range(7):
            daily_info_dict = compute_daily_hrs(
                work_hrs_df, self._week_dates_list[idx], tdelta_as_hrs_min=True
            )

            if daily_info_dict:
                self._shifts.append(daily_info_dict["shifts"])
                self._shift_deltas.append(daily_info_dict["shift_deltas"])
                self._shifts_tot.append(daily_info_dict["shifts_tot"])
                self._scheduled.append(daily_info_dict["scheduled"])
            else:
                self._shifts.append([""])
                self._shift_deltas.append(0)
                self._shifts_tot.append(0)
                self._scheduled.append(False)

    def get_info_by_day_of_week(self, idx):
        """return work info for day indexed by 0 (Mon) - 6 (Sun)"""
        return {
            "shifts": self._shifts[idx],
            "shift_deltas": self._shift_deltas[idx],
            "shifts_tot": self._shifts_tot[idx],
            "scheduled": self._scheduled[idx],
            "day": self._week_dates_list[idx],
        }

    def set_shifts_by_day_of_week(self, idx, shifts):
        """set shifts for day indexed by 0 (Mon) - 6 (Sun)"""
        self._shifts[idx] = shifts

    def get_week_dates_list(self):
        """return a list with the dates for each day Mon-Sun"""
        return self._week_dates_list

    def get_week_shifts_list(self):
        """return a list with shifts for each day Mon-Sun"""
        return self._shifts

    def get_week_scheduled_list(self):
        """return a list of scheduled flag for each shift for each day Mon-Sun"""
        return self._scheduled

    def __iter__(self):
        return WeekIterator(self)


class WeekIterator:
    def __init__(self, week):
        self._week = week
        self._index = 0

    def __next__(self):
        if self._index < len(self._week._week_dates_list):
            result = self._week.get_info_by_day_of_week(self._index)
            self._index += 1
            return result
        else:
            raise StopIteration


class WorkTimeRange:
    def __init__(self, work_hrs_df, dt_day_object_start, dt_day_object_stop):
        """A class which contains all days in a range of dates.

        :param work_hrs_df: The work hours DataFrame for a job
        :param dt_day_object_start: First day of the range
        :param dt_day_object_stop: Last day of the range
        """
        # create a list of dates for each day from the starting date to ending date
        self._dates_list = (
            pd.date_range(start=dt_day_object_start, end=dt_day_object_stop)
            .to_pydatetime()
            .tolist()
        )
        self._shifts = []
        self._shift_deltas = []
        self._shifts_tot = []
        self._scheduled = []
        self._num_shifts = []
        # compute and fill in work info from DataFrame for each day in the range
        for idx, date in enumerate(self._dates_list):
            daily_info_dict = compute_daily_hrs(
                work_hrs_df, date, tdelta_as_hrs_min=True
            )
            if daily_info_dict:
                self._shifts.append(daily_info_dict["shifts"])
                self._shift_deltas.append(daily_info_dict["shift_deltas"])
                self._shifts_tot.append(daily_info_dict["shifts_tot"])
                self._scheduled.append(daily_info_dict["scheduled"])
                self._num_shifts.append(len(self._shifts[idx]))
            else:
                self._shifts.append([""])
                self._shift_deltas.append([0])
                self._shifts_tot.append(0)
                self._scheduled.append([False])
                self._num_shifts.append(1)

    def get_info_by_day(self, idx):
        """return work info for day by index"""
        return {
            "num_shifts": self._num_shifts[idx],
            "shifts": self._shifts[idx],
            "shift_deltas": self._shift_deltas[idx],
            "shifts_tot": self._shifts_tot[idx],
            "scheduled": self._scheduled[idx],
            "day": self._dates_list[idx],
        }

    def __iter__(self):
        return TimeRangeIterator(self)


class TimeRangeIterator:
    def __init__(self, work_time_range):
        self._work_time_range = work_time_range
        self._index = 0

    def __next__(self):
        if self._index < len(self._work_time_range._dates_list):
            result = self._work_time_range.get_info_by_day(self._index)
            self._index += 1
            return result
        else:
            raise StopIteration


def get_report_str(
    bus_hrs_df,
    HD_hrs_df,
    delivery_hrs_df,
    dt_day_object_start,
    dt_day_object_stop,
    eight_day_df,
):
    """
    Create a report string for a range of dates based on work hours dataframes

    :param bus_hrs_df: Bus hours DataFrame
    :param HD_hrs_df: HD hours DataFrame
    :param delivery_hrs_df: delivery hours DataFrame
    :param dt_day_object_start: datetime object for the starting date
    :param dt_day_object_stop: datetime object for the ending date
    :param eight_day_df: the eight day window DataFrame
    :return: a report string with shift info and totals and an 8-day window sum
    """
    return_str = []
    title_str = (
        f"{'DATE:':^14}"
        f"     {'---Bus Shift---':^15} {'Shift':^5} {'Total':^5}"
        f"     {'---HD Shift---':^15} {'Shift':^5} {'Total':^5}"
        f"     {'--Del Shift--':^15} {'Shift':^5} {'Total':^5}"
        f"{'Daily Total':>20}"
        f"{'8-Day Rolling':>20}"
    )
    return_str.append(title_str)
    title_str_len = len(title_str)

    # Based on the date range and WorkTimeRange objects, create lists
    # of work information which will then be zipped together to create
    # a final report string
    for date, bus_info, HD_info, delivery_info in zip(
        pd.date_range(start=dt_day_object_start, end=dt_day_object_stop)
        .to_pydatetime()
        .tolist(),
        WorkTimeRange(bus_hrs_df, dt_day_object_start, dt_day_object_stop),
        WorkTimeRange(HD_hrs_df, dt_day_object_start, dt_day_object_stop),
        WorkTimeRange(delivery_hrs_df, dt_day_object_start, dt_day_object_stop),
    ):
        return_str.append("=" * title_str_len)
        if date.date() == dt.date.today():
            return_str.append("\nToday:")
            return_str.append("*" * title_str_len)
        date_str = date.strftime(DATE_FMT_STR)
        day_date_str = date.strftime("%a %Y-%m-%d")

        num_lines_to_print = max(
            bus_info["num_shifts"], HD_info["num_shifts"], delivery_info["num_shifts"]
        )

        date_list = [" " * len(day_date_str)] * num_lines_to_print

        bus_shifts_list = [" "] * num_lines_to_print
        bus_shift_hrs_list = [" "] * num_lines_to_print
        bus_tot_hrs_list = [" "] * num_lines_to_print

        HD_shifts_list = [" "] * num_lines_to_print
        HD_shift_hrs_list = [" "] * num_lines_to_print
        HD_tot_hrs_list = [" "] * num_lines_to_print

        delivery_shifts_list = [" "] * num_lines_to_print
        delivery_shift_hrs_list = [" "] * num_lines_to_print
        delivery_tot_hrs_list = [" "] * num_lines_to_print

        daily_tot_list = [" "] * num_lines_to_print
        eight_day_total_list = [" "] * num_lines_to_print

        date_list[0] = day_date_str
        bus_shifts_list[: len(bus_info["shifts"])] = bus_info["shifts"]
        bus_shift_hrs_list[: len(bus_info["shift_deltas"])] = bus_info["shift_deltas"]
        bus_tot_hrs_list[0] = bus_info["shifts_tot"]

        HD_shifts_list[: len(HD_info["shifts"])] = HD_info["shifts"]
        HD_shift_hrs_list[: len(HD_info["shift_deltas"])] = HD_info["shift_deltas"]
        HD_tot_hrs_list[0] = HD_info["shifts_tot"]

        delivery_shifts_list[: len(delivery_info["shifts"])] = delivery_info["shifts"]
        delivery_shift_hrs_list[: len(delivery_info["shift_deltas"])] = delivery_info[
            "shift_deltas"
        ]
        delivery_tot_hrs_list[0] = delivery_info["shifts_tot"]

        hrs, mins = compute_delta_hrs_min(
            eight_day_df[eight_day_df.date == date_str].eight_day_window.iloc[0]
        )
        eight_day_total_str = f"{hrs:02}:{mins:02}"
        eight_day_total_list[0] = eight_day_total_str

        hrs, mins = compute_delta_hrs_min(
            eight_day_df[eight_day_df.date == date_str].daily_tot_hrs.iloc[0]
        )
        daily_total_str = f"{hrs:02}:{mins:02}"
        daily_tot_list[0] = daily_total_str

        output_str_zip = zip(
            date_list,
            bus_shifts_list,
            bus_shift_hrs_list,
            bus_tot_hrs_list,
            HD_shifts_list,
            HD_shift_hrs_list,
            HD_tot_hrs_list,
            delivery_shifts_list,
            delivery_shift_hrs_list,
            delivery_tot_hrs_list,
            daily_tot_list,
            eight_day_total_list,
        )
        for line in output_str_zip:
            return_str.append(
                f"{line[0]:^10}"
                f"     {line[1]:^15} {line[2]:^5} {line[3]:^5}"
                f"     {line[4]:^15} {line[5]:^5} {line[6]:^5}"
                f"     {line[7]:^15} {line[8]:^5} {line[9]:^5}"
                f"{line[10]:>20}"
                f"{line[11]:>20}"
            )

        if date.date() == dt.date.today():
            return_str.append("*" * title_str_len + "\n")

    return "\n".join(return_str)


def get_notifications_str(dt_day_object_start, dt_day_object_stop, eight_day_df):
    """
    Create a notifications string based on 8-day rolling window DataFrame

    :param dt_day_object_start: datetime object for start date
    :param dt_day_object_stop: datetime object for end date
    :param eight_day_df: the 8-day rolling window DataFrame
    :return: a string containing alerts for the given range
    """
    range_window_df = eight_day_df[
        eight_day_df.date >= dt_day_object_start.strftime(DATE_FMT_STR)
    ]
    range_window_df = range_window_df[
        range_window_df.date <= dt_day_object_stop.strftime(DATE_FMT_STR)
    ]
    return display_alerts(range_window_df)


def check_for_scheduled_updates(work_hrs_df):
    """
    Check a work hours DataFrame to see what shifts before today still need to be updated with actual shift info

    :param work_hrs_df: a work hours DataFrame
    :return: a sorted lists of dates (strings) showing which dates need to be updated
    """
    today = dt.date.today()
    # reduce DataFrames to those dates before today that are still set as scheduled
    work_hrs_updates_df = work_hrs_df[work_hrs_df.date < today.strftime(DATE_FMT_STR)]
    work_hrs_updates_df = work_hrs_updates_df[work_hrs_updates_df.scheduled == 1]
    if work_hrs_updates_df.empty:
        return None
    else:

        return sorted(list(set(work_hrs_updates_df.date)))


def check_for_time_errors(work_hrs_df):
    """
    Check a work hours DataFrame for time formatting errors

    :param work_hrs_df: a work hours DataFrame
    :return: a string to notify of time formatting errors
    """
    work_hrs_df.sort_values(by="start", inplace=True)
    dates_to_check = sorted(list(set(work_hrs_df.date)))

    # check for valid time formats
    for idx, row in work_hrs_df.iterrows():
        try:
            # check for date matches
            dt_date = dt.datetime.strptime(row.date, DATE_FMT_STR).date()
            dt_start = dt.datetime.strptime(row.start, DATE_TIME_FMT_STR).date()
            dt_end = dt.datetime.strptime(row.end, DATE_TIME_FMT_STR).date()
            if (dt_date != dt_start) or (dt_date != dt_end):
                return "Date mismatches"
        except ValueError:
            return "Date/time format issue"

    if dates_to_check:
        for date in dates_to_check:
            date_df = work_hrs_df[work_hrs_df.date == date]
            date_start = date_df.start.tolist()
            date_end = date_df.end.tolist()

            for start, end in zip(date_start, date_end):
                if start >= end:
                    return "Date order issues - same shift"

            if len(date_start) > 1:
                date_end.pop(-1)
                date_start.pop(0)
                for start, end in zip(date_start, date_end):
                    if date_end >= date_start:
                        return "Date order issues - overlapping shifts"

    return ""


def process_manifest(manifest_text, work_hrs_df):
    """
    parse text representing a work manifest, create a Week object with shift info filled in

    :param manifest_text: a string of text grabbed from an image of a work manifest
    :param work_hrs_df: a work hours DataFrame
    :return: a Week object with manifest hours filled in
    """
    shifts = []
    date_week_of_str = None
    # parse text for the starting date of the week
    for line in manifest_text.split("\n"):
        if "Coord" in line:
            m = re.search(r"(\d{2}/\d{2})", line)
            if m:
                date_week_of_str = m.groups()[0]
                logger.error("Coord found.")

        # parse text for the work shift info, military hours format (1500-1800)
        m = re.findall(r"(\d{4}-\d{4})", line)
        if m:
            for item in m:
                shifts.append(item)
                logger.error("Coord date found")


    if not date_week_of_str or not shifts or (len(shifts) != 10):
        logger.error("No coord info found")
        if not date_week_of_str:
            logger.error("date_week_of_str missing")
        if not shifts:
            logger.error("shifts missing")
        if len(shifts) != 10:
            logger.error("shifts not 10: {}", len(shifts))
        return None

    # add colons to shifts (1500-1800 -> 15:00-18:00)
    for idx, shift in enumerate(shifts):
        shifts[idx] = shift[:2] + ":" + shift[2:7] + ":" + shift[7:]

    # create a datetime object based on the date pulled from manifest
    # this date is only month/year, so year is assumed to be this year
    # unless this makes the date in the past, therefore year is next year
    today = dt.datetime.now()
    week_of_date = dt.datetime.strptime(date_week_of_str, "%m/%d")
    week_of_date = week_of_date.replace(year=today.year)
    if week_of_date < today:
        week_of_date = week_of_date.replace(week_of_date.year + 1)

    # create a Week object with this date
    work_info = Week(work_hrs_df, week_of_date)

    # swap the morning and afternoon shifts to be in time order
    for idx in range(5):
        if shifts[idx].split("-")[0] > shifts[idx + 5].split("-")[0]:
            shifts[idx], shifts[idx + 5] = shifts[idx + 5], shifts[idx]

    # remove any 00:00-00:00 shifts
    for idx, shift in enumerate(shifts):
        if shift == '00:00-00:00':
            shifts[idx] = ''

    # modify the Week object to have the new shift info
    for idx in range(5):
        work_info.set_shifts_by_day_of_week(idx, [shifts[idx], shifts[idx + 5]])

    return work_info
