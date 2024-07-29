import pandas as pd
import numpy as np


class DataTools:
    """
    A class with static methods that help with manipulating and analyzing DataFrames.

    Methods
    -------
    reorder_columns(df, columns, inserted_column_fill=np.nan)
        Reorder the columns of a DataFrame and insert missing columns with a specified fill value.
    """

    @staticmethod
    def reorder_columns(df: pd.DataFrame, columns: list[str], inserted_column_fill: any = np.nan) -> pd.DataFrame:
        """
        Reorder the columns of a DataFrame and insert missing columns with a specified fill value.

        Parameters
        ----------
        df : pd.DataFrame
            The input DataFrame to reorder.
        columns : list of str
            The list of column names in the desired order. Columns not present in the DataFrame will be added.
        inserted_column_fill : any, optional
            The value to fill in for inserted columns that are not present in the original DataFrame (default
            is np.nan, which is a null value).

        Returns
        -------
        pd.DataFrame
            A new DataFrame with columns reordered and missing columns filled with the specified value.
        """

        # Create a copy of the original DataFrame to avoid modifying it
        return_df = df.copy()

        # Iterate through the desired column order
        for column in columns:
            # Add missing columns with the specified fill value
            if column not in df.columns:
                return_df.loc[:, column] = inserted_column_fill

        # Return the DataFrame with columns reordered according to the specified order
        return return_df.loc[:, columns]
