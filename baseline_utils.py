
import numpy as np
import pandas as pd


def calculate_excluding_current_baseline(
    df,
    bucket_columns,
    value_column="total_activity"
):
    """
    Calculate a median baseline within each bucket while
    excluding the current observation.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data containing bucket columns and value column.

    bucket_columns : list[str]
        Columns defining the comparison bucket.

        Examples:
            NP3: ["grid_id", "date"]
            ML4: ["grid_id", "hour"]

    value_column : str
        Numeric activity column used to calculate the baseline.

    Returns
    -------
    pandas.Series
        Baseline value aligned to the original dataframe index.

    Important:
        The current observation is excluded from its own baseline.
        This preserves the original NP3 behavior.
    """

    baseline = pd.Series(
        np.nan,
        index=df.index,
        dtype=float
    )

    grouped = df.groupby(
        bucket_columns,
        sort=False
    )

    for _, group in grouped:

        values = group[value_column].to_numpy(
            dtype=float
        )

        indices = group.index.to_numpy()

        for position, index in enumerate(indices):

            other_values = np.delete(
                values,
                position
            )

            if len(other_values) > 0:

                baseline.loc[index] = np.median(
                    other_values
                )

    return baseline
