#############################################################
# work_hours.py - A program to track and report my work hours
# written by Mark Alexander (alexander.markv@gmail.com)
#############################################################
import PySimpleGUI as sg
import datetime as dt
import copy
import work_hrs_help as wh
import pandas as pd
from loguru import logger
import re

DATE_FMT_STR = "%Y-%m-%d"

work_hours_buttons_layout = [
    [
        sg.Button(
            "Bus Shifts",
            mouseover_colors="#fed800",  # fed800: National School Bus Yellow
            expand_x=True,
            key="-BUSHRS-",
        )
    ],
    [
        sg.Button(
            "HD Shifts",
            mouseover_colors="#ee7125",  # ee7125: Home Depot orange
            expand_x=True,
            key="-HDHRS-",
        )
    ],
    [
        sg.Button(
            "Delivery Shifts",
            mouseover_colors="green",
            expand_x=True,
            key="-DLVRHRS-",
        )
    ],
]

report_buttons_layout = [
    [sg.Push(), sg.Button("View 8-day Report", key="-8DAYREPORT-")],
    [sg.Push(), sg.Button("View Future Report", key="-FUTUREREPORT-")],
]

main_layout = [
    [
        sg.Multiline(
            "",
            key="-HRS_OUTPUT-",
            expand_x=True,
            expand_y=True,
            no_scrollbar=True,
            write_only=True,
            reroute_stdout=False,
        )
    ],
    [
        sg.Column(work_hours_buttons_layout),
        sg.Push(),
        sg.Frame(
            "Notifications",
            [
                [
                    sg.Multiline(
                        "",
                        no_scrollbar=True,
                        write_only=True,
                        key="-NOTIFICATIONS-",
                        size=(100, 20),
                        reroute_stdout=True, # print statements will redirect to this window
                    )
                ]
            ],
            title_location="n",
        ),
        sg.Push(),
        sg.Column(report_buttons_layout),
    ],
    [sg.Push(), sg.Text(f"Today is: {dt.datetime.today().strftime('%A %B %d, %Y')}", font=('Arial', 15))]
]

shifts_window_layout = [
    [
        sg.Button("PREV", key="-PREV-"),
        sg.Button("NEXT", key="-NEXT-"),
        sg.Button("This Week", key="-TODAY-"),
        sg.Push(),
        sg.Button("Manifest", key="-MANIFEST-", visible=False),
    ],
    # [
    #     sg.Text("", key="-WEEKOF-")
    # ],
    [
        sg.Frame(
            "Shifts: HH:MM-HH:MM",
            [
                [
                    sg.Text("", key="-TEXT_0-"),
                    sg.Push(),
                    sg.Column(
                        [
                            [sg.InputText("", key="-INPUT_0_A-")],
                            [sg.InputText("", key="-INPUT_0_B-")],
                            [sg.InputText("", key="-INPUT_0_C-")],
                        ]
                    ),
                ],
                [
                    sg.Text("", key="-TEXT_1-"),
                    sg.Push(),
                    sg.Column(
                        [
                            [sg.InputText("", key="-INPUT_1_A-")],
                            [sg.InputText("", key="-INPUT_1_B-")],
                            [sg.InputText("", key="-INPUT_1_C-")],
                        ]
                    ),
                ],
                [
                    sg.Text("", key="-TEXT_2-"),
                    sg.Push(),
                    sg.Column(
                        [
                            [sg.InputText("", key="-INPUT_2_A-")],
                            [sg.InputText("", key="-INPUT_2_B-")],
                            [sg.InputText("", key="-INPUT_2_C-")],
                        ]
                    ),
                ],
                [
                    sg.Text("", key="-TEXT_3-"),
                    sg.Push(),
                    sg.Column(
                        [
                            [sg.InputText("", key="-INPUT_3_A-")],
                            [sg.InputText("", key="-INPUT_3_B-")],
                            [sg.InputText("", key="-INPUT_3_C-")],
                        ]
                    ),
                ],
                [
                    sg.Text("", key="-TEXT_4-"),
                    sg.Push(),
                    sg.Column(
                        [
                            [sg.InputText("", key="-INPUT_4_A-")],
                            [sg.InputText("", key="-INPUT_4_B-")],
                            [sg.InputText("", key="-INPUT_4_C-")],
                        ]
                    ),
                ],
                [
                    sg.Text("", key="-TEXT_5-"),
                    sg.Push(),
                    sg.Column(
                        [
                            [sg.InputText("", key="-INPUT_5_A-")],
                            [sg.InputText("", key="-INPUT_5_B-")],
                            [sg.InputText("", key="-INPUT_5_C-")],
                        ]
                    ),
                ],
                [
                    sg.Text("", key="-TEXT_6-"),
                    sg.Push(),
                    sg.Column(
                        [
                            [sg.InputText("", key="-INPUT_6_A-")],
                            [sg.InputText("", key="-INPUT_6_B-")],
                            [sg.InputText("", key="-INPUT_6_C-")],
                        ]
                    ),
                ],
            ],
            title_location="n",
            key="-FRAME-",
        )
    ],
    [
        sg.Button("Save Changes", key="-SAVE-"),
        sg.Button("Cancel", key="-CANCEL-"),
        sg.Text("Status:", key="-STATUS-", text_color="#FFFFFF"),
    ],
]


def write_to_window(window, work_info):
    """
    Update work hours window with info given by a Week object

    :param window: window object to be written to
    :param work_info: Week object with info to be written to the window
    :return: None
    """
    work_info = copy.deepcopy(work_info)
    for key_num, date in enumerate(work_info.get_week_dates_list()):
        # the first row in the Week object is Monday, start of our week
        if key_num == 0:
            date_str = date.strftime("%A %B %d, %Y")
            window["-FRAME-"].update(f"Week of {date_str}")
        window[f"-TEXT_{key_num}-"].update(date.strftime("%A %b %d, %Y"))

    # write current shift information to input windows, red text if scheduled
    for key_num, (shifts, scheduled) in enumerate(
        zip(work_info.get_week_shifts_list(), work_info.get_week_scheduled_list())
    ):
        if len(shifts) < 3:
            shifts.extend(" " * (3 - len(shifts)))
        if scheduled and (1 in scheduled):
            scheduled_text_color = "red"
        else:
            scheduled_text_color = "black"
        for shift_idx, key_letter in zip([0,1,2], ['A', 'B', 'C']):
            window[f"-INPUT_{key_num}_{key_letter}-"].update(
                shifts[shift_idx], text_color=scheduled_text_color
            )


def read_shifts_window(values, work_info):
    """
    Read shift info for the week indicated by the work hours window.

    :param values: values from the work hours window to be read
    :param work_info: Week object with info originally in the work hours window
    :return: A new dataframe with the info read from the work hours window
    """
    new_shifts = []
    # read shift info (HH:MM-HH:MM) from shift windows
    for idx in range(7):
        shift_list = []
        for letter in ["A", "B", "C"]:
            shift = values[f"-INPUT_{idx}_{letter}-"]
            if shift != " " and shift != "":
                shift_list.append(shift)
        if len(shift_list) == 0:
            shift_list.append("")
        new_shifts.append(shift_list)

    new_df_dict = {"date": [], "start": [], "end": [], "scheduled": []}
    # build dictionary of dates, start times, end times, and scheduled flags
    # dictionary used to build a DataFrame
    week_dates_list = work_info.get_week_dates_list()
    for idx, shifts in enumerate(new_shifts):
        for shift in shifts:
            if shift != " " and shift != "":
                m = re.match(r'^\d{2}:\d{2}-\d{2}:\d{2}$', shift)
                if not m:
                    return None
                shift = shift.split("-")
                date_str = week_dates_list[idx].strftime("%Y-%m-%d")
                new_df_dict["date"].append(date_str)
                new_df_dict["start"].append(date_str + " " + shift[0])
                new_df_dict["end"].append(date_str + " " + shift[1])
                if date_str > dt.date.today().strftime("%Y-%m-%d"):
                    new_df_dict["scheduled"].append(1)
                else:
                    new_df_dict["scheduled"].append(0)

    new_df = pd.DataFrame(new_df_dict)

    return new_df


def work_hrs_window(db_table, manifest_button=False):
    """
    A PySimpleGUI window which contains shift info for a week

    :param db_table: database table name
    :param manifest_button: Control visibility of a manifest button
    :return: A string representing any status updates
    """
    work_hrs_layout = copy.deepcopy(shifts_window_layout)

    today = dt.date.today()
    work_hrs_df = wh.read_work_hrs_table(db_table)
    work_info = wh.Week(work_hrs_df, today)
    return_str = ""
    window = sg.Window(
        f"{db_table}", work_hrs_layout, modal=True, keep_on_top=True, finalize=True
    )
    if manifest_button:
        window["-MANIFEST-"].update(visible=True)

    write_to_window(window, work_info)
    changes_flag = False
    while True:
        event, values = window.read()
        if event == "-CANCEL-" or event == sg.WIN_CLOSED:
            break
        if event == "-PREV-" or event == "-NEXT-" or event == "-TODAY-":
            if event == "-PREV-":
                today = today - dt.timedelta(days=7)
            if event == "-NEXT-":
                today = today + dt.timedelta(days=7)
            if event == "-TODAY-":
                today = dt.date.today()
            work_info = wh.Week(work_hrs_df, today)
            write_to_window(window, work_info)
        if event == "-MANIFEST-":
            manifest_text = sg.popup_get_text(
                "Enter manifest text here.",
                title="Manifest Text",
                keep_on_top=True,
                size=(50, 50),
            )
            if manifest_text:
                work_info = wh.process_manifest(manifest_text, work_hrs_df)
                write_to_window(window, work_info)

        if event == "-SAVE-":
            new_work_hrs_df = read_shifts_window(values, work_info)

            if new_work_hrs_df is None:
                # update STATUS text with error
                window["-STATUS-"].update("Formatting error", text_color="#FF0000")
                continue

            for date in work_info.get_week_dates_list():
                # for each date, create a filtered DataFrame of the new
                # and existing work info to detect changes to be saved
                work_hrs_date_df = work_hrs_df[
                    work_hrs_df.date == date.strftime("%Y-%m-%d")
                ][["date", "start", "end", "scheduled"]].reset_index()
                new_work_hrs_date_df = new_work_hrs_df[
                    new_work_hrs_df.date == date.strftime("%Y-%m-%d")
                ][["date", "start", "end", "scheduled"]].reset_index()

                # detect changes 1) either DataFrame empty while other isn't,
                # 2) number of entries are different, 3) some start times differ,
                # 4) some end times differ, 5) scheduled flags differ
                if (
                    (work_hrs_date_df.empty != new_work_hrs_date_df.empty)
                    or (work_hrs_date_df.shape[0] != new_work_hrs_date_df.shape[0])
                    or not (work_hrs_date_df.start == new_work_hrs_date_df.start).all()
                    or not (work_hrs_date_df.end == new_work_hrs_date_df.end).all()
                    or not (
                        work_hrs_date_df.scheduled == new_work_hrs_date_df.scheduled
                    ).all()
                ):
                    changes_flag = True

                    time_errors_str = wh.check_for_time_errors(new_work_hrs_date_df)
                    # time_errors_str = ""
                    if time_errors_str != "":
                        # update STATUS text with error
                        window["-STATUS-"].update(time_errors_str, text_color="#FF0000")
                        changes_flag = "Stay"
                        break
                    else:
                        wh.write_work_hrs_table(
                            db_table,
                            work_hrs_date_df,
                            new_work_hrs_date_df,
                        )

            if changes_flag is True:
                return_str = "Changes saved"
                break
            elif changes_flag == "Stay":
                logger.error("Save failed, still in {} hours window.", db_table)
                changes_flag = False
            else:
                return_str = "No changes saved"
                break

    window.close()
    return return_str


def main_window():
    window = sg.Window(
        "Work Hours", main_layout, resizable=True, size=(1600, 1000), finalize=True
    )

    first_loop_flag = True
    while True:
        event, values = window.read(timeout=100)

        # Read in hours tables
        bus_hrs_df = wh.read_work_hrs_table("bus_hours")
        HD_hrs_df = wh.read_work_hrs_table("HD_hours")
        delivery_hrs_df = wh.read_work_hrs_table("delivery_hours")

        if first_loop_flag:
            first_loop_flag = False
            output_str = ""
            bus_update_dates_list = wh.check_for_scheduled_updates(bus_hrs_df)
            if bus_update_dates_list:
                output_str = (
                    "Bus hours need to be updated:\n"
                    + "\n".join(bus_update_dates_list)
                    + "\n"
                )
            HD_update_dates_list = wh.check_for_scheduled_updates(HD_hrs_df)
            if HD_update_dates_list:
                output_str += (
                    "HD hours need to be updated:\n"
                    + "\n".join(HD_update_dates_list)
                    + "\n"
                )
            if output_str != "":
                # print update needed notices to NOTIFICATIONS window
                print(output_str)

        # Compute an eight day window DataFrame
        eight_day_df = wh.compute_eight_day_df(bus_hrs_df, HD_hrs_df, delivery_hrs_df)

        if event == sg.WIN_CLOSED:
            break

        if event == "-BUSHRS-":
            return_str = work_hrs_window("bus_hours", manifest_button=True)
            if return_str != "":
                # print notices to the NOTIFICATIONS window
                print(return_str)

        if event == "-HDHRS-":
            return_str = work_hrs_window("HD_hours")
            # print notices to the NOTIFICATIONS window
            if return_str != "":
                print(return_str)

        if event == "-DLVRHRS-":
            return_str = work_hrs_window("delivery_hours")
            # print notices to the NOTIFICATIONS window
            if return_str != "":
                print(return_str)

        if event == "-8DAYREPORT-":
            window["-HRS_OUTPUT-"].update("")
            window["-NOTIFICATIONS-"].update("")
            seven_days_ago = dt.datetime.today() - dt.timedelta(days=7)
            seven_day_from_now = dt.datetime.today() + dt.timedelta(days=7)
            window["-HRS_OUTPUT-"].update(
                wh.get_report_str(
                    bus_hrs_df,
                    HD_hrs_df,
                    delivery_hrs_df,
                    seven_days_ago,
                    seven_day_from_now,
                    eight_day_df,
                )
            )
            display_str = wh.get_notifications_str(
                seven_days_ago, seven_day_from_now, eight_day_df
            )
            # print notices to the NOTIFICATIONS window
            print(display_str)

        if event == "-FUTUREREPORT-":
            window["-HRS_OUTPUT-"].update("")
            window["-NOTIFICATIONS-"].update("")
            max_date = max(
                max(bus_hrs_df.date), max(HD_hrs_df.date), max(delivery_hrs_df.date)
            )
            window["-HRS_OUTPUT-"].update(
                wh.get_report_str(
                    bus_hrs_df,
                    HD_hrs_df,
                    delivery_hrs_df,
                    dt.date.today(),
                    max_date,
                    eight_day_df,
                )
            )
            display_str = wh.get_notifications_str(
                dt.date.today(),
                dt.datetime.strptime(max_date, "%Y-%m-%d"),
                eight_day_df,
            )
            # print notices to the NOTIFICATIONS window
            print(display_str)


if __name__ == "__main__":
    main_window()
