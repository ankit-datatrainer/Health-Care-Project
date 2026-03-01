import pandas as pd
from typing import List


class DimDatePreprocessor:
    """
    A class to generate a Date dimension table from multiple DataFrames 
    containing 'Date' columns.

    Methods
    -------
    generate_dim_date(list_of_df: List[pd.DataFrame]) -> pd.DataFrame
        Creates a complete date dimension table based on the combined date range in the input.
    """

    REQUIRED_COLUMNS = {'Date'}

    @staticmethod
    def generate_dim_date(list_of_df: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Generate a Date dimension table based on date ranges from the input DataFrames.

        Parameters
        ----------
        list_of_df : List[pd.DataFrame]
            A non-empty list of DataFrames, each with a 'Date' column.

        Returns
        -------
        pd.DataFrame
            A Date dimension table with enriched temporal fields.
        """

        # Validate input
        if (
            not isinstance(list_of_df, list)
            or len(list_of_df) == 0
            or not all(isinstance(df, pd.DataFrame) for df in list_of_df)
        ):
            raise ValueError("Input must be a non-empty list of pandas DataFrames.")

        # Check for 'Date' column in each DataFrame
        for i, df in enumerate(list_of_df):
            missing = DimDatePreprocessor.REQUIRED_COLUMNS - set(df.columns)
            if missing:
                raise ValueError(f"DataFrame at index {i} is missing column(s): {missing}")

        # Combine all dates into one Series
        all_dates = pd.concat([df['Date'] for df in list_of_df])
        all_dates = pd.to_datetime(all_dates).dropna().dt.date

        # Get unique dates and full range
        start_date = min(all_dates)
        end_date = max(all_dates)
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        # Build Dim_Date DataFrame
        dim_date = pd.DataFrame({
            'Date': date_range.date,
            'Year': date_range.year,
            'Month': date_range.month,
            'MonthName': date_range.strftime('%B'),
            'Day': date_range.day,
            'DayName': date_range.strftime('%A'),
            'Week': date_range.isocalendar().week,
            'Weekday': date_range.day_name(),
            'Quarter': date_range.quarter,
            'YearMonth': date_range.strftime('%Y-%m'),
            'YearMonthName': date_range.strftime('%Y - %B'),
            'DateID': date_range.strftime('%Y%m%d').astype(int),
            'Month Year': date_range.strftime('%b %Y')
        })

        return dim_date
