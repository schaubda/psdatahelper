import pandas as pd
import numpy as np


class DataTools:
    """
    TODO: Fill in the docstring for DataTools
    DataTools is a class that contains static methods for manipulating data in a DataFrame.
    """

    @staticmethod
    def reorder_columns(df: pd.DataFrame, columns: list[str], inserted_column_fill: any = np.nan) -> pd.DataFrame:
        """
        TODO: Fill in the docstring for reorder_columns
        Reorders columns in a DataFrame to the order specified in the columns list. If a column in the columns list
        :param df:
        :param columns:
        :param inserted_column_fill:
        :return:
        """
        return_df = df.copy()

        for column in columns:
            if column not in df.columns:
                return_df.loc[:, column] = inserted_column_fill

        return return_df.loc[:, columns]
