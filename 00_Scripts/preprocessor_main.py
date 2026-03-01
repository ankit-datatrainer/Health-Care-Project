#---Importing Libraries
import pandas as pd
import numpy as np
import os
import sys
import datetime 

from Fact_Appointment_Preprocessor import AppointmentPreprocessor
from Fact_Consultation_Preprocessor import ConsultationPreprocessor
from Fact_Patientreg_Preprocessor import PatientRegPreprocessor
from Fact_Phclogin_Preprocessor import PHCLoginPreprocessor

from Generate_Dim_PHC import DimPHCPreprocessor
from Generate_Dim_Doctor import DimDoctorPreprocessor
from Generate_Dim_Date import DimDatePreprocessor

from FactTableTransformer import FactTableTransformer

from workbook_utils import WorkbookUtils

if __name__ == "__main__":
    #---Reading Raw Datasets

    # Raw files Path
    raw_data_path = os.path.join(os.getcwd(), r'../01_DataSources/RAW')

    # Reading & Preprocessing Apointment Report
    Appointment_df = WorkbookUtils.read_workbooks(os.path.join(raw_data_path, r'Appointment Reports'))
    Appointment_df = AppointmentPreprocessor.preprocess(Appointment_df)

    # Reading & Preprocessing Patient Registeration Reports
    Patientreg_df = WorkbookUtils.read_workbooks(os.path.join(raw_data_path, r'Patient Registration'))
    Patientreg_df = PatientRegPreprocessor.preprocess(Patientreg_df)

    # Reading & Preprocessing Consultations Report
    Consultation_df = WorkbookUtils.read_workbooks(os.path.join(raw_data_path, r'Consultation Reports'))
    Consultation_df = ConsultationPreprocessor.preprocess(Consultation_df)

    # Reading PHC Login
    Phclogin_df = WorkbookUtils.read_workbooks(os.path.join(raw_data_path, r'PHC Login Report'))
    Phclogin_df = PHCLoginPreprocessor.preprocess(Phclogin_df)


    # Generating Dim_PHC
    Dim_PHC = DimPHCPreprocessor.generate_dim_phc(list_of_df = [Consultation_df, Patientreg_df, Appointment_df, Phclogin_df])

    # Generating Dim_Doctor
    Dim_Doctor = DimDoctorPreprocessor.generate_dim_doctor([Consultation_df, Appointment_df])

    #  Creating Dim Date
    Dim_Date = DimDatePreprocessor.generate_dim_date([Consultation_df, Appointment_df, Phclogin_df, Patientreg_df])



    Appointment_df = FactTableTransformer.transform_appointment(df = Appointment_df, dim_phc=Dim_PHC, dim_date=Dim_Date, dim_doctor=Dim_Doctor)
    Consultation_df = FactTableTransformer.transform_consultation(df = Consultation_df, dim_phc=Dim_PHC, dim_date=Dim_Date, dim_doctor=Dim_Doctor)
    Phclogin_df = FactTableTransformer.transform_phc_login(df = Phclogin_df, dim_phc=Dim_PHC, dim_date=Dim_Date)
    Patientreg_df = FactTableTransformer.transform_patient_registration(df = Patientreg_df, dim_phc=Dim_PHC, dim_date=Dim_Date)

    Dim_Doctor['Doctor'] = 'Doctor' + " " +Dim_Doctor['DoctorID'].astype(str).str.zfill(2)
    Dim_PHC['PHCName'] = 'PHC' + " " +Dim_PHC['PHCID'].astype(str).str.zfill(2)

    #Save Preprocessed Data
    Appointment_df.to_parquet(os.path.join(os.getcwd(), r'../01_DataSources/Processed\Processed_Appointment.parquet'), engine="pyarrow", index=False)
    Patientreg_df.to_parquet(os.path.join(os.getcwd(), r'../01_DataSources/Processed\Processed_Patientreg.parquet'), engine="pyarrow", index=False)
    Phclogin_df.to_parquet(os.path.join(os.getcwd(), r'../01_DataSources/Processed\Processed_PHCLogin.parquet'), engine="pyarrow", index=False)
    Consultation_df.to_parquet(os.path.join(os.getcwd(), r'../01_DataSources/Processed\Processed_Consultation.parquet'), engine="pyarrow", index=False)

    Dim_PHC.to_parquet(os.path.join(os.getcwd(), r'../01_DataSources/Processed\Processed_Dim_PHC.parquet'), engine="pyarrow", index=False)
    Dim_Doctor.to_parquet(os.path.join(os.getcwd(), r'../01_DataSources/Processed\Processed_Dim_Doctor.parquet'), engine="pyarrow", index=False)
    Dim_Date.to_parquet(os.path.join(os.getcwd(), r'../01_DataSources/Processed\Processed_Dim_Date.parquet'), engine="pyarrow", index=False)
