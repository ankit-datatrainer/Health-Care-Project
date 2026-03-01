import datetime
import numpy as np
import pandas as pd
from workbook_utils import WorkbookUtils

class ConsultationPreprocessor:
    """
    A utility class for preprocessing consultation data, including:
    - Parsing dates and time
    - Calculating call durations
    - Converting and categorizing patient age
    - Renaming and cleaning columns
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
            Age group label (e.g., 'Infant', 'Adult'). Returns 'Unknown' if no match.
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
    def preprocess(df):
        """
        Preprocess the consultation DataFrame by:
        - Parsing and combining dates and times
        - Calculating call durations and validating them
        - Converting and categorizing patient ages
        - Cleaning, renaming, and dropping unnecessary columns

        Parameters
        ----------
        df : pd.DataFrame
            The raw consultation data.

        Returns
        -------
        pd.DataFrame
            A cleaned, sampled DataFrame with relevant consultation info.
        """
        cols_to_drop = [
            'SrNo', 'Cluster', 'PatientName', 'Age',
            'EndTime', 'PatientCaseID', 'ReferredBy',
            'Designation', 'LTName', 'Qualification', 'ApprovalDate',
            'complaint', 'WorkbookName', 'Age (in Years)'
        ]

        # Parse ConsultDate
        df["ConsultDate"] = pd.to_datetime(df["ConsultDate"], format="%d-%m-%Y").dt.date

        # Combine date with time and parse datetime
        df["StartTime"] = pd.to_datetime(df["ConsultDate"].astype(str) + " " + df["StartTime"])
        df["EndTime"] = pd.to_datetime(df["ConsultDate"].astype(str) + " " + df["EndTime"])

        # Calculate call duration and status
        df["Call Duration"] = (df["EndTime"] - df["StartTime"]).dt.total_seconds()
        df["Status: Consultation"] = np.where(df["Call Duration"] < 120, "Invalid Call", "Valid Call")
        

        df['StartTime'] = df['StartTime'].dt.hour
        df["StartTime"] = df["StartTime"].astype(str).str.zfill(2) + " - " + ((df["StartTime"] + 1) % 24).astype(str).str.zfill(2)

        # Process age
        age_split = df["Age"].str.extract(r"(\d+)\s*(\w+)")
        df["Age (in Years)"] = np.where(
            age_split[1].str.lower().str.startswith("year"),
            age_split[0].astype(float),
            age_split[0].astype(float) / 365
        )
        df["Age (in Years)"] = df["Age (in Years)"].astype(int)

        # Categorize age
        df["Age_grp"] = df["Age (in Years)"].apply(ConsultationPreprocessor.categorize_age)

        # Rename columns
        df = df.rename(columns={
            'District': 'DistrictName',
            'Block': 'BlockName',
            'ConsultDate': 'Date',
            'StartTime' : 'Hour',
            'PHC': 'PHCName',
            'Patient': 'PatientName'
        })
        
        # Drop duplicates and unnecessary columns
        df = df.drop_duplicates(subset='PatientCaseID', ignore_index=True)
        df = df.drop(columns=cols_to_drop)

        df['OPDNo'] = df['OPDNo'].str.strip().apply(lambda x: int(x) if str(x).strip().isnumeric() else np.nan)
        df = df.dropna(subset = 'OPDNo')
        df['OPDNo'] = df['OPDNo'].astype(int)
        
        # Step 1: Ensure 'OPDNo' column is of string type
        df['OPDNo'] = df['OPDNo'].astype(str)
        
        # Step 2: Create mapping from unique OPD strings to sequential numbers
        unique_opds = df['OPDNo'].unique()
        opd_mapping = {opd: idx + 1 for idx, opd in enumerate(unique_opds)}
        
        # Step 3: Apply the mapping using .map (faster than .replace)
        df['OPDNo'] = df['OPDNo'].map(opd_mapping)
        
        
        # Sample for analysis
        return df
