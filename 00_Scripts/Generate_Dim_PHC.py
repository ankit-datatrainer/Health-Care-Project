import pandas as pd
from typing import List


class DimPHCPreprocessor:
    """
    A class to preprocess and generate a PHC (Primary Health Center) dimension table
    from multiple raw data sources.

    Attributes
    ----------
    list_of_df : List[pd.DataFrame]
        A list containing DataFrames with 'DistrictName', 'BlockName', 'PHCName'.

    Methods
    -------
    generate_dim_phc() -> pd.DataFrame
        Combines, cleans, and enriches PHC data into a single dimension table.
    """

   

    @staticmethod
    def generate_dim_phc(list_of_df) -> pd.DataFrame:
        """
        Generate a PHC dimension table by combining unique rows from all input DataFrames.

        Returns
        -------
        pd.DataFrame
            A dimension table containing unique PHCs with assigned IDs, state, country, and division info.
        """
        
        REQUIRED_COLUMNS = {'DistrictName', 'BlockName', 'PHCName'}
        
        if (
            not isinstance(list_of_df, list)
            or len(list_of_df) == 0
            or not all(isinstance(df, pd.DataFrame) for df in list_of_df)
        ):
            raise ValueError("Input must be a non-empty list of pandas DataFrames.")

        for i, df in enumerate(list_of_df):
            missing = REQUIRED_COLUMNS - set(df.columns)
            if missing:
                raise ValueError(f"DataFrame at index {i} is missing columns: {missing}")

        REQUIRED_COLUMNS = list(REQUIRED_COLUMNS)
        
        # Concatenate and drop duplicates
        dim_phc = pd.concat(
            [df[REQUIRED_COLUMNS] for df in list_of_df],
            ignore_index=True
        ).drop_duplicates()

        # Add PHCID
        dim_phc['PHCID'] = range(1, len(dim_phc) + 1)

        # Add static fields
        dim_phc['State'] = 'Madhya Pradesh'
        dim_phc['Country'] = 'India'

        # Add Division
        division_mapping = {
            'Bhopal': ['Betul', 'Bhopal', 'Harda', 'Hoshangabad', 'Raisen', 'Rajgarh', 'Sehore', 'Vidisha'],
            'Ujjain': ['Agar Malwa', 'Dewas', 'Mandsaur', 'Shajapur', 'Ujjain', 'Neemuch', 'Ratlam'],
            'Indore': ['Dhar', 'Indore', 'Khargone', 'EAST NIMAR', 'Barwani', 'Burhanpur', 'Jhabua', 'Alirajpur'],
            'Gwalior': ['Guna', 'Gwalior', 'Morena', 'Sheopur', 'Shivpuri', 'Ashoknagar', 'Bhind', 'Datia']
        }

        # Reverse map: district → division
        district_to_division = {
            district: division
            for division, districts in division_mapping.items()
            for district in districts
        }

        # Map division using district
        dim_phc['Division'] = dim_phc['DistrictName'].map(district_to_division)

        return dim_phc
