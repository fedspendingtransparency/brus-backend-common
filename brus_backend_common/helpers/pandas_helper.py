import logging
import pandas as pd
import numpy as np

from brus_backend_common.helpers.generic_helper import get_utc_now
from brus_backend_common.helpers.script_helper import FailureThresholdExceededError

logger = logging.getLogger(__name__)

FAILURE_THRESHOLD_PERCENTAGE = 0.01


def trim_item(item):
    if isinstance(item, str):
        return item.strip()
    return item


def pad_function(field, pad_to, keep_null):
    """Pads field to specified length."""
    if pd.isnull(field) or not str(field).strip():
        if keep_null:
            return None
        else:
            field = ""
    return str(field).strip().zfill(pad_to)


def clean_col_names(field):
    """Define some data-munging functions that can be applied to pandas
    dataframes as necessary"""
    return str(field).lower().strip().replace(" ", "_").replace(",", "_")


def clean_data(data, field_map, field_options, required_values=[], return_dropped_count=False):
    """Cleans up a dataframe that contains domain values.

    Args:
        data: dataframe of domain values
        field_map: dict that maps columns of the dataframe csv to our model columns
        field_options: dict with keys of attribute names, value contains a dict with options for that attribute.
            Current options are:
             "pad_to_length" which if present will pad the field with leading zeros up to
            specified length
            "keep_null" when set to true, empty fields will not be padded
            "skip_duplicate" which ignores subsequent lines that repeat values
            "strip_commas" which removes commas
        required_values: list of required values
        return_dropped_count: flag to return dropped count

    Returns:
        Dataframe conforming to requirements, and additionally the number of rows dropped for missing value
        if return_dropped_count argument is True

    Raises:
        FailureThresholdExceededError: If too many rows have been discarded during processing, fail the routine.
           Also fail if the file is blank.

    """
    # incoming .csvs often have extraneous blank rows at the end,
    # so get rid of those
    clean_df = data.copy(deep=True)
    clean_df.dropna(inplace=True, how="all")

    # clean the dataframe column names
    clean_df.rename(columns=clean_col_names, inplace=True)
    # make sure all values in fieldMap parameter are in the dataframe/csv file
    for field in field_map:
        if field not in list(clean_df.columns):
            raise ValueError(f"{field} is required per field_map")
    # toss out any columns from the csv that aren't in the fieldMap parameter
    clean_df = clean_df[list(field_map.keys())]
    # rename columns as specified in fieldMap
    clean_df = clean_df.rename(columns=field_map)

    # trim all columns
    clean_df = clean_df.map(lambda x: trim_item(x) if len(str(x).strip()) else None)

    if len(required_values) > 0:
        # if file is blank, immediately fail
        if clean_df.empty or len(clean_df.shape) < 2:
            raise FailureThresholdExceededError(0)
        # check the columns that must have a valid value, and if they have white space,
        # replace with NaN so that dropna finds them.
        for value in required_values:
            clean_df[value].replace("", np.nan, inplace=True)
        # drop any rows that are missing required data
        cleaned = clean_df.dropna(subset=required_values)
        dropped = clean_df[np.invert(clean_df.index.isin(cleaned.index))]
        # log every dropped row
        for index, row in dropped.iterrows():
            logger.info(
                f"Dropped row due to faulty data: "
                f"fyq:{row['fiscal_year_period']}"
                f"--agency:{row['agency_id']}"
                f"--alloc:{row['allocation_transfer_id']}"
                f"--account:{row['account_number']}"
                f"--pa_code:{row['program_activity_code']}"
                f"--pa_name:{row['program_activity_name']}"
            )

        if (len(dropped.index) / len(clean_df.index)) > FAILURE_THRESHOLD_PERCENTAGE:
            raise FailureThresholdExceededError(len(dropped.index))
        logger.info("{} total rows dropped due to faulty data".format(len(dropped.index)))
        clean_df = cleaned

    # apply column options as specified in fieldOptions param
    for col, options in field_options.items():
        if "pad_to_length" in options:
            # pad to specified length
            clean_df[col] = clean_df[col].apply(pad_function, args=(options["pad_to_length"], options.get("keep_null")))
        if options.get("strip_commas"):
            # remove commas for specified column
            # get rid of commas in dollar amounts
            clean_df[col] = clean_df[col].str.replace(",", "")

    # add created_at and updated_at columns
    now = get_utc_now()
    clean_df = clean_df.assign(created_at=now, updated_at=now)
    if return_dropped_count:
        return len(dropped.index), clean_df
    return clean_df


def check_dataframe_diff(new_data, model, del_cols, sort_cols, lambda_funcs=None, date_format="%m/%d/%Y"):
    """Checks if 2 dataframes (the new data and the existing data for a model) are different.

    Args:
        new_data: dataframe containing the new data to compare
        model: The DeltaModel to get the existing data from
        del_cols: An array containing the columns to delete from the existing data (usually id and foreign keys)
        sort_cols: An array containing the columns to sort on
        lambda_funcs: An array of tuples (column to update, transformative lambda taking in a row argument)
                      that will be processed in the order provided.
        date_format: The format into which date columns will be converted to strings

    Returns:
        True if there are differences between the two dataframes, false otherwise
    """
    if not lambda_funcs:
        lambda_funcs = {}

    new_data_copy = new_data.copy(deep=True)

    # Drop the created_at and updated_at columns from the new data so they don't cause differences
    try:
        new_data_copy.drop(["created_at", "updated_at"], axis=1, inplace=True)
    except (ValueError, KeyError):
        logger.info("created_at or updated_at column not found, drop skipped.")

    current_data = model.to_df()

    # Apply any lambda functions provided to update values if needed
    if not current_data.empty:
        for col_name, lambda_func in lambda_funcs:
            current_data[col_name] = current_data.apply(lambda_func, axis=1)

    # Drop the created_at and updated_at for the same reason as above, also drop the pk ID column for this table
    try:
        current_data.drop(["created_at", "updated_at"] + del_cols, axis=1, inplace=True)
    except (ValueError, KeyError):
        logger.info(
            "created_at, updated_at, or at least one of the columns provided for deletion not found," " drop skipped."
        )

    # Convert all dates to strings for proper comparison
    for col in new_data_copy.select_dtypes(include=["datetime64"]).columns.tolist():
        new_data_copy[col] = new_data_copy[col].dt.strftime(date_format)
    for col in current_data.select_dtypes(include=["datetime64"]).columns.tolist():
        current_data[col] = current_data[col].dt.strftime(date_format)

    # Convert all remaining columns to strings for proper comparison
    new_data_copy = new_data_copy.replace(np.nan, "").astype(str)
    current_data = current_data.replace(np.nan, "").astype(str)

    # Replace all NaT/nan strings created from above changes with ''
    new_data_copy = new_data_copy.replace("[Nn]a[Tn]", "", regex=True).astype(str)
    current_data = current_data.replace("[Nn]a[Tn]", "", regex=True).astype(str)

    # Strip all strings so they don't have extra whitespace
    new_data_copy = new_data_copy.map(lambda x: x.strip() if isinstance(x, str) else x)
    current_data = current_data.map(lambda x: x.strip() if isinstance(x, str) else x)

    # pandas comparison requires everything to be in the same order
    new_data_copy.sort_values(by=sort_cols, inplace=True)
    current_data.sort_values(by=sort_cols, inplace=True)

    # Columns have to be in order too
    cols = new_data_copy.columns.tolist()
    cols.sort()
    new_data_copy = new_data_copy[cols]

    cols = current_data.columns.tolist()
    cols.sort()
    current_data = current_data[cols]

    # Reset indexes after sorting, so that they match
    new_data_copy.reset_index(drop=True, inplace=True)
    current_data.reset_index(drop=True, inplace=True)

    return not new_data_copy.equals(current_data)
