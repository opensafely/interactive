from datetime import date, datetime
import numpy as np
from dateutil import relativedelta
from cohortextractor import (
    StudyDefinition,
    codelist,
    patients,
)

from variables import study_start_date, study_end_date


def build_study_definition_for_counts(codelist):
    start_date = datetime.strptime(study_start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(study_end_date, "%Y-%m-%d").date()

    return StudyDefinition(
        default_expectations={
            "date": {"earliest": study_start_date, "latest": study_end_date},
            "rate": "uniform",
            "incidence": 0.5,
        },
        index_date=study_start_date,
        population=patients.satisfying(
            "currently_registered OR has_died",
            currently_registered=patients.registered_as_of(study_end_date),
            has_died=patients.with_death_recorded_in_primary_care(
                between=[study_start_date, study_end_date], returning="binary_flag"
            ),
        ),
        **calculate_code_frequency(start_date, end_date, codelist),
    )


def calculate_code_frequency(start_date, end_date, selected_codes):
    start_date_formatted = date.strftime(start_date, "%Y-%m-%d")
    end_date_formatted = date.strftime(end_date, "%Y-%m-%d")

    variables = {}
    for code in selected_codes:
        variables[f"code_{code}"] = patients.with_these_clinical_events(
            codelist([code], system="snomed"),
            between=[start_date_formatted, end_date_formatted],
            episode_defined_as="series of events each <= 0 days apart",
            returning="number_of_episodes",
            return_expectations={
                "incidence": 0.1,
                "int": {"distribution": "normal", "mean": 3, "stddev": 1},
            },
        )
    return variables


def last_day_of_month(dt):
    return dt + relativedelta.relativedelta(day=31)


def last_day_of_week(dt):
    return dt + relativedelta.relativedelta(day=7)


def group_low_values(df, count_column, code_column, threshold):
    """Suppresses low values and groups suppressed values into
    a new row "Other".

    Args:
        df: A measure table of counts by code.
        count_column: The name of the count column in the measure table.
        code_column: The name of the code column in the codelist table.
        threshold: Redaction threshold to use
    Returns:
        A table with redacted counts
    """

    # get sum of any values <= threshold
    suppressed_count = df.loc[df[count_column] <= threshold, count_column].sum()

    # if no values are suppressed we don't need to do anything
    if suppressed_count == 0:
        pass

    # if suppressed values >0 ensure total suppressed count > threshold
    else:
        # redact counts <= threshold
        df.loc[df[count_column] <= threshold, count_column] = np.nan

        # if suppressed count <= threshold redact further values
        while suppressed_count <= threshold:
            suppressed_count += df[count_column].min()
            df.loc[df[count_column].idxmin(), :] = np.nan

        # drop all rows where count column is null
        df = df.loc[df[count_column].notnull(), :]

        # add suppressed count as "Other" row (if > threshold)
        if suppressed_count > threshold:
            suppressed_count = {code_column: "Other", count_column: suppressed_count}
            df = df.append(suppressed_count, ignore_index=True)

    return df


def create_top_5_code_table(
    df, code_df, code_column, term_column, low_count_threshold, rounding_base, nrows=5
):
    """Creates a table of the top 5 codes recorded with the number of events and % makeup of each code.
    Args:
        df: A measure table.
        code_df: A codelist table.
        code_column: The name of the code column in the codelist table.
        term_column: The name of the term column in the codelist table.
        measure: The measure ID.
        low_count_threshold: Value to use as threshold for disclosure control.
        rounding_base: Base to round to.
        nrows: The number of rows to display.
    Returns:
        A table of the top `nrows` codes.
    """

    # cast both code columns to str
    df[code_column] = df[code_column].astype(str)
    code_df[code_column] = code_df[code_column].astype(str)

    # sum event counts over patients
    event_counts = df.sort_values(ascending=False, by="num")

    event_counts = group_low_values(
        event_counts, "num", code_column, low_count_threshold
    )

    # round

    event_counts["num"] = event_counts["num"].apply(
        lambda x: round_values(x, rounding_base)
    )

    # calculate % makeup of each code
    total_events = event_counts["num"].sum()
    event_counts["Proportion of codes (%)"] = round(
        (event_counts["num"] / total_events) * 100, 2
    )

    # Gets the human-friendly description of the code for the given row
    # e.g. "Systolic blood pressure".
    code_df[code_column] = code_df[code_column].astype(str)
    code_df = code_df.set_index(code_column).rename(
        columns={term_column: "Description"}
    )

    event_counts = event_counts.set_index(code_column).join(code_df).reset_index()

    # set description of "Other column" to something readable
    event_counts.loc[event_counts[code_column] == "Other", "Description"] = "-"

    # Rename the code column to something consistent
    event_counts.rename(columns={code_column: "Code"}, inplace=True)

    # drop events column
    event_counts = event_counts.loc[
        :, ["Code", "Description", "Proportion of codes (%)"]
    ]

    # return top n rows
    return event_counts.head(5)


def calculate_rate(df, value_col, rate_per=1000, round_rate=False):
    """Calculates the number of events per 1,000 of the population.
    This function operates on the given measure table in-place, adding
    a `rate` column.
    Args:
        df: A measure table.
        value_col: The name of the numerator column in the measure table.
        population_col: The name of the denominator column in the measure table.
        round: Bool indicating whether to round rate to 2dp.
    """
    if round_rate:
        rate = round(df[value_col] * rate_per, 2)

    else:
        rate = df[value_col] * rate_per

    return rate


def round_values(x, base=5):
    if x is not None and not str(x).startswith("<=") and not np.isnan(x):
        rounded = int(base * round(x / base))
    else:
        rounded = np.nan
    return rounded


def drop_irrelevant_practices(df):
    """Drops irrelevant practices from the given measure table.
    An irrelevant practice has zero events during the study period.
    Args:
        df: A measure table.
    Returns:
        A copy of the given measure table with irrelevant practices dropped.
    """
    is_relevant = df.groupby("practice").value.any()
    return df[df.practice.isin(is_relevant[is_relevant == True].index)]


def redact_events_table(events_counts, low_count_threshold, rounding_base):
    # redact low counts
    events_counts[events_counts <= low_count_threshold] = f"<={low_count_threshold}"

    # round
    events_counts["count"] = events_counts["count"].apply(
        lambda x: round_values(x, base=rounding_base)
    )

    return events_counts
