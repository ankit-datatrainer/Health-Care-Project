import pandas as pd
from workbook_utils import WorkbookUtils

class AppointmentPreprocessor:
 
    @staticmethod
    def preprocess(df, year_month=None):
        
        consult_status = df.ConsultStatus.unique()
        df = df.pivot_table(index = ['AppointmentTime', 'DistrictName', 'BlockName', 'PHCName', 
                                    'Doctor', 'Specialization'], 
                            values = 'PatientName', aggfunc='count', columns = 'ConsultStatus').fillna(0).astype(int).reset_index()
        
        df['Count: Appointments'] = df[consult_status].sum(axis = 1)
        df = df.rename(columns = {'AppointmentTime' : 'Date', })
        df['Date'] = pd.to_datetime(df['Date'], format = '%d-%m-%Y').dt.date
        return df
