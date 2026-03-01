import pandas as pd

class PatientRegPreprocessor:
    """
    A utility class for preprocessing patient registration data.

    Methods
    -------
    categorize_age(age: int) -> str
        Categorizes a numeric age into a defined age group.

    preprocess(df: pd.DataFrame, year_month: str = None) -> pd.DataFrame
        Processes the patient registration DataFrame by transforming age data,
        parsing dates, grouping data, and returning a sampled result.
    """

    @staticmethod
    def categorize_age(age):
        """
        Categorize the given age into a predefined age group.

        Parameters
        ----------
        age : int or float
            The age in years to categorize.

        Returns
        -------
        str
            A string label for the age group (e.g., 'Infant', 'Adult').
            Returns 'Unknown' if the age doesn't match any defined range.
        """
        age_groups = [
            ((0, 2), 'Infant'),
            ((3, 5), 'Preschool child'),
            ((6, 13), 'Child'),
            ((14, 18), 'Adolescent'),
            ((19, 64), 'Adult'),
            ((65, float('inf')), 'Senior')
        ]

        for (start, end), label in age_groups:
            if start <= age <= end:
                return label
        return 'Unknown'

    @staticmethod
    def preprocess(df, year_month=None):
        """
        Preprocess the patient registration DataFrame.

        Operations include:
        - Converting age from text to integer years.
        - Categorizing patients into age groups.
        - Filtering out unknown or invalid age entries.
        - Converting registration dates to datetime objects.
        - Pivoting the data to get daily counts of patient registrations.
        - Sampling the final DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            The raw patient registration DataFrame.

        year_month : str, optional
            Reserved for future use (e.g., filtering by year-month).

        Returns
        -------
        pd.DataFrame
            A transformed and sampled DataFrame ready for analysis.
        """
        cols_to_drop = ['Age']
        
        # Preprocessing Age
        df['Age'] = df['Age'].apply(
            lambda x: int(x.split()[0]) if 'year' in x.lower() else int(x.split()[0]) / 365
        )
        df['Age'] = df['Age'].astype(int)
        df['Age_grp'] = df['Age'].apply(PatientRegPreprocessor.categorize_age)

        # Dropping Age Values with Unknown
        df = df.drop(index=df[df['Age_grp'] == 'Unknown'].index).reset_index(drop=True)

        # Preprocessing Date
        df['Hour'] = pd.to_datetime(df['Registration Date'], format='%d-%m-%Y %I:%M:%S %p').dt.hour
        df['Hour'] = df["Hour"].astype(str).str.zfill(2) + " - " + ((df["Hour"] + 1) % 24).astype(str).str.zfill(2)
        df['Date'] = pd.to_datetime(df['Registration Date'], format='%d-%m-%Y %I:%M:%S %p').dt.date
        
        
        # Processing Data
        df = df.pivot_table(
            index=['Date', 'District', 'Block', 'PHC', 'Gender', 'Age_grp', 'Hour'],
            aggfunc='count',
            values='Patient Name'
        ).reset_index()
        df = df.rename(columns={'Patient Name': 'Count: Patient Registered', 
                               'District' : 'DistrictName', 'Block' : 'BlockName', 
                               'PHC' : 'PHCName'})



        return df
