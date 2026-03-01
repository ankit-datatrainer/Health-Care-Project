import pandas as pd
from workbook_utils import WorkbookUtils

class PHCLoginPreprocessor:
    @staticmethod
    def preprocess(df, year_month=None):
        df['Date'] = pd.to_datetime(df['Date'], format = '%d-%m-%Y').dt.date
        
        df['Login Time'] = pd.to_datetime(df['Date'].astype(str) + " " + df['Login Time'], format = '%Y-%m-%d %H:%M:%S')
        df['Logout Time'] = pd.to_datetime(df['Date'].astype(str) + " " + df['Logout Time'], format = '%Y-%m-%d %H:%M:%S')
        df['PHC Uptime'] = (df['Logout Time'] - df['Login Time'])
        
        df = df.drop(columns = ['SL No.', 'Cluster', 'LT Name', 
                                    'Qualification', 'Approval Date', 'Phase', 'Location', 
                                    'Login Time', 'Logout Time', 'Duration(hh:mm:ss)', 
                                    'Remark', 'WorkbookName'])
        df = df.rename(columns = {'District' : 'DistrictName', 'Block' : 'BlockName', 'PHC' : 'PHCName'})
        return df