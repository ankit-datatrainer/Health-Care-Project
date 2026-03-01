import pandas as pd
import numpy as np
from typing import List


class DimDoctorPreprocessor:
    """
    A class to preprocess and generate a Doctor dimension table
    from multiple raw data sources.

    Attributes
    ----------
    REQUIRED_COLUMNS : set
        Required columns in each input DataFrame ('Doctor', 'Specialization').

    Methods
    -------
    generate_dim_doctor(list_of_df: List[pd.DataFrame]) -> pd.DataFrame
        Combines, deduplicates, and enriches doctor data with IDs and assigned hubs.
    """

    REQUIRED_COLUMNS = {'Doctor', 'Specialization'}
    HUBS = ['Indore', 'Delhi', 'Pune', 'Agra', 'Nagpur']

    @staticmethod
    def generate_dim_doctor(list_of_df: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Generate a Doctor dimension table by combining data from multiple sources.

        Parameters
        ----------
        list_of_df : List[pd.DataFrame]
            A non-empty list of DataFrames containing 'Doctor' and 'Specialization' columns.

        Returns
        -------
        pd.DataFrame
            A cleaned doctor dimension table with unique rows, assigned DoctorIDs, and random Hubs.
        """

        # Validate input
        if (
            not isinstance(list_of_df, list)
            or len(list_of_df) == 0
            or not all(isinstance(df, pd.DataFrame) for df in list_of_df)
        ):
            raise ValueError("Input must be a non-empty list of pandas DataFrames.")

        # Validate required columns in each DataFrame
        for i, df in enumerate(list_of_df):
            missing = DimDoctorPreprocessor.REQUIRED_COLUMNS - set(df.columns)
            if missing:
                raise ValueError(f"DataFrame at index {i} is missing columns: {missing}")

        # Concatenate and drop duplicates
        required_cols = list(DimDoctorPreprocessor.REQUIRED_COLUMNS)
        dim_doctor = pd.concat(
            [df[required_cols] for df in list_of_df],
            ignore_index=True
        ).drop_duplicates()

        # Add DoctorID
        dim_doctor['DoctorID'] = range(1, len(dim_doctor) + 1)

        # Add random Hub
        dim_doctor['Hub'] = np.random.choice(DimDoctorPreprocessor.HUBS, size=len(dim_doctor))

        return dim_doctor
