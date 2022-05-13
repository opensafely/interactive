import pytest
import pandas as pd
import numpy as np
from pandas import testing
import study_utils
from hypothesis import strategies as st
from hypothesis import assume, given


@pytest.fixture()
def measure_table():
    """Returns a measures table produced by generate_measures.py."""
    return pd.DataFrame(
        {
            "date": pd.Series(
                ["2019-01-01", "2019-01-01", "2019-01-01", "2019-02-01", "2019-02-01"]
            ),
            "practice": pd.Series([1, 2, 3, 1, 2]),
            "num": pd.Series([3, 10, 1, 0, 4]),
            "list_size": pd.Series([10, 20, 4, 2, 8]),
            "value": pd.Series([0, 1, 1, 0, 1]),
        }
    )


top_5_codes_params = [
    # no low numbers
    {
        "obs": {"code": ["01", "02", "03"], "num": [2, 80, 18]},
        "exp": {
            "Code": ["02", "Other"],
            "Description": ["code 2", "-"],
            "Proportion of codes (%)": [80.0, 20.0],
        },
    },
    # all low numbers (and total < threshold)
    {
        "obs": {"code": ["01", "02", "03"], "num": [2, 1, 1]},
        "exp": {"Code": [], "Description": [], "Proportion of codes (%)": []},
    },
    # all low numbers (and total > threshold)
    {
        "obs": {"code": ["01", "02", "03"], "num": [2, 3, 4]},
        "exp": {
            "Code": ["Other"],
            "Description": ["-"],
            "Proportion of codes (%)": [100.0],
        },
    },
    # low numbers with sum > total
    {
        "obs": {"code": ["01", "02", "03"], "num": [4, 4, 10]},
        "exp": {
            "Code": ["03", "Other"],
            "Description": ["code 3", "-"],
            "Proportion of codes (%)": [50.0, 50.0],
        },
    },
    # low numbers with sum < total
    {
        "obs": {"code": ["01", "02", "03"], "num": [2, 2, 10]},
        "exp": {
            "Code": ["Other"],
            "Description": ["-"],
            "Proportion of codes (%)": [100.0],
        },
    },
]

events_counts_params = [
    # no low numbers
    {
        "obs": {"count": [100, 8, 16]},
        "exp": {"count": [100, 10, 15]},
    },
    # all low numbers
    {
        "obs": {"count": [4, 1, 1]},
        "exp": {"count": [np.nan, np.nan, np.nan]},
    },
    # some low numbers
    {
        "obs": {"count": [12, 2, 3]},
        "exp": {"count": [10, np.nan, np.nan]},
    },
]


@pytest.fixture()
def codelist():
    """Returns a codelist like table."""
    return pd.DataFrame(
        {
            "code": pd.Series(["01", "02", "03"]),
            "term": pd.Series(["code 1", "code 2", "code 3"]),
        }
    )


@pytest.mark.parametrize("top_5_codes_params", top_5_codes_params)
def test_create_top_5_code_table(top_5_codes_params, codelist):

    # make a counts table
    counts_per_code_table = pd.DataFrame(
        {
            "code": pd.Series(top_5_codes_params["obs"]["code"]),
            "num": pd.Series(top_5_codes_params["obs"]["num"]),
        }
    )

    # make top 5 table using counts table
    obs = study_utils.create_top_5_code_table(
        counts_per_code_table, codelist, "code", "term", 5, 5
    )

    # get expected top 5 table
    exp = pd.DataFrame(
        {
            "Code": pd.Series(top_5_codes_params["exp"]["Code"]),
            "Description": pd.Series(top_5_codes_params["exp"]["Description"]),
            "Proportion of codes (%)": pd.Series(
                top_5_codes_params["exp"]["Proportion of codes (%)"]
            ),
        },
    )

    # below fixes the typing if expected df is empty
    if exp["Code"].empty:
        exp["Code"] = exp["Code"].astype(str)
        exp["Description"] = exp["Description"].astype(str)
        exp["Proportion of codes (%)"] = exp["Proportion of codes (%)"].astype(float)

    testing.assert_frame_equal(obs, exp)


@pytest.mark.parametrize("events_counts_table", events_counts_params)
def test_redact_events_table(events_counts_table):

    # make events table

    events_table = pd.DataFrame(
        {"count": pd.Series(events_counts_table["obs"]["count"])},
        index=["total_events", "events_in_latest_period", "unique_patients"],
    )

    obs = study_utils.redact_events_table(events_table, 5, 5)

    exp = pd.DataFrame(
        {"count": pd.Series(events_counts_table["exp"]["count"])},
        index=["total_events", "events_in_latest_period", "unique_patients"],
    )

    testing.assert_frame_equal(obs, exp)


@st.composite
def distinct_strings_with_common_characters(draw):
    count_column = draw(
        st.lists(st.one_of(st.none(), st.integers(min_value=0, max_value=1000000)))
    )
    code_column = draw(st.lists(st.text(min_size=1)))
    assume(len(count_column) == len(code_column))

    count_column_name = draw(st.text(min_size=1))
    code_column_name = draw(st.text(min_size=1))
    assume(count_column_name != code_column_name)
    df = pd.DataFrame(
        data={count_column_name: count_column, code_column_name: code_column}
    )
    return df


@given(
    distinct_strings_with_common_characters(), st.integers(min_value=0, max_value=10)
)
def test_group_low_values(df, threshold):
    count_column, code_column = df.columns
    result = study_utils.group_low_values(df, count_column, code_column, threshold)

    assert list(result.columns) == list(df.columns)
    assert not (result[count_column] < threshold).any()
    suppressed_count = result[result[code_column] == "Other"][count_column]
    assert len(suppressed_count) <= 1
    if len(suppressed_count) == 1:
        assert suppressed_count[0] >= threshold


@st.composite
def random_events_table(draw):
    total_events = draw(
        st.one_of(st.none(), st.integers(min_value=0, max_value=1000000))
    )
    events_in_latest_period = draw(
        st.one_of(st.none(), st.integers(min_value=0, max_value=1000000))
    )
    unique_patients = draw(
        st.one_of(st.none(), st.integers(min_value=0, max_value=1000000))
    )

    df = pd.DataFrame(
        {
            "total_events": total_events,
            "events_in_latest_period": events_in_latest_period,
            "unique_patients": unique_patients,
        },
        index=["count"],
    )
    return df


@given(
    random_events_table(),
    st.integers(min_value=1, max_value=1000),
    st.integers(min_value=1, max_value=10),
)
def test_redact_events_table_hypothesis(df, threshold, rounding_base):
    result = study_utils.redact_events_table(df.T, threshold, rounding_base)

    assert list(result.columns) == list(df.T.columns)
    assert not (result["count"] < threshold).any()
