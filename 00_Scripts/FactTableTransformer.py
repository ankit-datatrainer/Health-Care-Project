import pandas as pd


class FactTableTransformer:
    """
    A class to transform fact tables by replacing descriptive columns with foreign keys
    from corresponding dimension tables (PHC, Doctor, Date).
    """

    @staticmethod
    def transform_appointment(df: pd.DataFrame, dim_phc: pd.DataFrame, dim_date: pd.DataFrame, dim_doctor: pd.DataFrame) -> pd.DataFrame:
        df = pd.merge(df, dim_phc[['PHCName', 'PHCID']], on='PHCName', how='left')
        df = pd.merge(df, dim_date[['Date', 'DateID']], on='Date', how='left')
        df = pd.merge(df, dim_doctor[['Doctor', 'Specialization', 'DoctorID']], on=['Doctor', 'Specialization'], how='left')
        
        return df.drop(columns=['PHCName', 'Date', 'Doctor', 'Specialization', 'DistrictName', 'BlockName'])

    @staticmethod
    def transform_patient_registration(df: pd.DataFrame, dim_phc: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
        df = pd.merge(df, dim_phc[['PHCName', 'PHCID']], left_on='PHCName', right_on='PHCName', how='left')
        df = pd.merge(df, dim_date[['Date', 'DateID']], on='Date', how='left')
        return df.drop(columns=['PHCName', 'Date',  'DistrictName', 'BlockName'])

    @staticmethod
    def transform_consultation(df: pd.DataFrame, dim_phc: pd.DataFrame, dim_date: pd.DataFrame, dim_doctor: pd.DataFrame) -> pd.DataFrame:
        df = pd.merge(df, dim_phc[['PHCName', 'PHCID']], on='PHCName', how='left')
        df = pd.merge(df, dim_date[['Date', 'DateID']], on='Date', how='left')
        df = pd.merge(df, dim_doctor[['Doctor', 'Specialization', 'DoctorID']], on=['Doctor', 'Specialization'], how='left')
        return df.drop(columns=['PHCName', 'Date', 'Doctor', 'Specialization',  'DistrictName', 'BlockName'])

    @staticmethod
    def transform_phc_login(df: pd.DataFrame, dim_phc: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
        df = pd.merge(df, dim_phc[['PHCName', 'PHCID']], left_on='PHCName', right_on='PHCName', how='left')
        df = pd.merge(df, dim_date[['Date', 'DateID']], on='Date', how='left')
        return df.drop(columns=['PHCName', 'Date',  'DistrictName', 'BlockName'])

    @staticmethod
    def check_missing_keys(df: pd.DataFrame, key_columns: list[str]):
        """
        Utility method to print missing foreign key joins.
        """
        for col in key_columns:
            if df[col].isnull().any():
                missing = df[df[col].isnull()]
                print(f"⚠️ Warning: Missing foreign key values in column '{col}' — {len(missing)} unmatched rows.")
