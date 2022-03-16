import configparser
import operator
import string
from datetime import date, datetime, timedelta

import click
import gitlab
from gspread.auth import service_account_from_dict
from gspread.exceptions import WorksheetNotFound

LABEL_DOING = "status::Doing"
LABEL_REVIEW = "status::Review"
LABEL_READY_FOR_QA = "status::Ready for Final QA"
LABEL_WAITING_FOR_PROD = "status::Waiting for PROD"
LABEL_RELEASED = "status::Released"
LABELS = {
    LABEL_DOING: "Doing",
    LABEL_REVIEW: "Review",
    LABEL_READY_FOR_QA: "QA",
    LABEL_WAITING_FOR_PROD: "Waiting for Prod",
    LABEL_RELEASED: "Released",
}
# Label transitions we care about, (starting_label, final_label)
LABEL_TRANSITIONS = (
    # "Small steps"
    (LABEL_DOING, LABEL_REVIEW),
    (LABEL_REVIEW, LABEL_READY_FOR_QA),
    (LABEL_READY_FOR_QA, LABEL_WAITING_FOR_PROD),
    (LABEL_WAITING_FOR_PROD, LABEL_RELEASED),
    # Overall flow
    (LABEL_DOING, LABEL_RELEASED),
)


def load_config():
    config = configparser.ConfigParser(interpolation=None)
    config.read("config.cfg")
    return config


config = load_config()


def get_gitlab_object():
    return gitlab.Gitlab(
        config["gitlab:auth"]["url"], private_token=config["gitlab:auth"]["api_token"]
    )


gl = get_gitlab_object()


def get_gspread_client():
    return service_account_from_dict(dict(config["gsheets:auth"]))


gc = get_gspread_client()


@click.group()
def cli():
    pass


def parse_datetime(datetime_str):
    """
    Parse datetime str returned by the api into a datetime object
    Format is Format is 2022-02-02T13:39:28.926Z
    """
    return datetime.fromisoformat(datetime_str.rstrip("Z"))


def format_datetime_for_gsheet(datetime_obj):
    """
    See https://support.google.com/docs/answer/3093039
    """
    if datetime_obj is None:
        return None

    return datetime_obj.strftime("%m/%d/%Y %H:%M:%S")


def write_to_gsheet(values, *, tab_name):
    sheet = gc.open_by_url(config["ghseets"]["spreadsheet_url"])
    nb_rows = len(values)
    nb_cols = len(values[0])

    # Get or create the tab
    try:
        worksheet = sheet.worksheet(tab_name)
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows=nb_rows, cols=nb_cols)

    # This will break if we have more than 26 columns, heh :shrug:
    worksheet.update(f"A1:{string.ascii_uppercase[nb_cols]}{nb_rows}", values)


def find_events_dates_changes(events, *, starting_label, final_label):
    """
    Given a list of sorted label events, return a tuple:
    - start_datetime: the latest datetime at which the starting_label was
                      applied, before final_label has been applied
    - end_datetime: the earliest datetime at which the final_label was applied
    """
    # Iterate over events to find the dates
    start_datetime = None
    end_datetime = None
    for label, created_at in events:
        # We stop the first time we find the final label
        if label == final_label:
            end_datetime = created_at
            break
        # We always update the start_datetime since we want the latest one
        elif label == starting_label:
            start_datetime = created_at

    return (start_datetime, end_datetime)


def find_issue_dates_changes(issue, *, start_date, end_date):
    """
    Look at the issue's label events and returns a list of tuples for each label transition:
    - start_datetime: the latest datetime at which the starting_label was
                      applied, before final_label has been applied
    - end_datetime: the earliest datetime at which the final_label was applied

    We only consider tuples for which `end_datetime` is within `start_date` and `end_date`
    Some values within the tuple may be None: either the transitions didn't happen during
    the desired window, or the transition just didn't happen at all
    """
    # Filter on the relevant events
    events = [
        (event.label["name"], parse_datetime(event.created_at))
        for event in issue.resourcelabelevents.list(as_list=False)
        if event.label and event.label["name"] in LABELS.keys()
    ]

    # Sort by created date (might already be the case? It's not clear in the doc)
    events = sorted(events, key=operator.itemgetter(1))

    values = []
    for starting_label, final_label in LABEL_TRANSITIONS:
        start_datetime, end_datetime = find_events_dates_changes(
            events, starting_label=starting_label, final_label=final_label
        )
        if (
            start_datetime
            and end_datetime
            and not (start_date <= end_datetime < end_date)
        ):
            start_datetime = None
            end_datetime = None

        values.append((start_datetime, end_datetime))

    return values


def find_project_date_changes(project, *, start_date, end_date):
    """
    Look at of all the project's issues label events and returns a list of tuples:
    - Issue #
    Then for each label transition:
    - start_datetime in str form
    - end_datetime in str form
    - nb. days between end_datetime and start_datetime

    See `find_issue_dates_changes` for more details on start_date, end_date and label transitions

    TODO: Right now we look at *all* of a project's issues, ideally we would do some filtering
    """
    values = []

    # We iterate over the issues
    for idx, issue in enumerate(project.issues.list(as_list=False)):
        if idx and idx % 100 == 0:
            click.echo(f"Treated {idx} issues")

        # We extract start and end datetimes
        date_changes = find_issue_dates_changes(
            issue,
            start_date=start_date,
            end_date=end_date,
        )

        issue_values = []
        # We format the datetimes and add the nb of days
        for start_datetime, end_datetime in date_changes:
            # Handle None values
            if start_datetime is None or end_datetime is None:
                issue_values += ["", "", ""]
            else:
                issue_values += [
                    format_datetime_for_gsheet(start_datetime),
                    format_datetime_for_gsheet(end_datetime),
                    (end_datetime - start_datetime).total_seconds() / (3600 * 24),
                ]

        # If all values are empty, let's skip the line
        if not any(issue_values):
            continue

        # Otherwise we prepend the line with the issue number and add it to the result
        issue_values.insert(0, issue.iid)
        values.append(issue_values)

    return values


@cli.command(short_help="Compute stats for label changes")
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today() - timedelta(days=30)),
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today()),
)
@click.option("--tab-name")
def generate_label_report(start_date, end_date, tab_name):
    # Header for the data
    header = ["Project", "Issue #"]
    for starting_label, final_label in LABEL_TRANSITIONS:
        header.append(LABELS[starting_label])
        header.append(LABELS[final_label])
        header.append("Nb. days")

    values = [header]

    # Iterate over each project
    for project_name, project_id in config["gitlab:projects"].items():
        click.echo(f"Looking at {project_name}")
        project = gl.projects.get(project_id)
        project_values = find_project_date_changes(
            project,
            start_date=start_date,
            end_date=end_date,
        )

        # Prepend each list with the project name
        for v in project_values:
            v.insert(0, project.name)
            values.append(v)

    # Push all that into the Gsheet
    write_to_gsheet(values, tab_name=tab_name)


if __name__ == "__main__":
    cli()
