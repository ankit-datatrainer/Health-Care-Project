import os
import pandas as pd

class WorkbookUtils:
    @staticmethod
    def get_file_list(path, ext="*", return_df=False):
        file_list = [
            os.path.join(root, file)
            for root, _, files in os.walk(path)
            for file in files
        ]

        if ext != "*":
            file_list = [f for f in file_list if os.path.splitext(f)[-1] in ext]

        file_list = [f for f in file_list if os.path.isfile(f)]

        if not return_df:
            return file_list

        return pd.DataFrame({
            'Path': file_list,
            'FileName': [os.path.basename(p) for p in file_list],
            'FileSize (in bytes)': [os.path.getsize(p) for p in file_list]
        })

    @staticmethod
    def read_workbook(wb_path, sheet_name=None):
        ext = os.path.splitext(wb_path)[-1].lower()
        if ext == ".csv":
            df = pd.read_csv(wb_path, low_memory=False)
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(wb_path, sheet_name=sheet_name) if sheet_name else pd.read_excel(wb_path)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

        df['WorkbookName'] = wb_path
        df.columns = df.columns.str.strip()
        return df

    @staticmethod
    def read_workbooks(loc, sheet_name=None, verbose=False):
        dfs = []
        if os.path.isdir(loc):
            file_list = WorkbookUtils.get_file_list(loc, ['.csv', '.xlsx'])
            for file in file_list:
                if verbose:
                    print(f"- Reading: {os.path.basename(file)}; Sheet: {sheet_name}")
                dfs.append(WorkbookUtils.read_workbook(file, sheet_name))
        elif os.path.isfile(loc):
            if verbose:
                print(f"- Reading: {os.path.basename(loc)}; Sheet: {sheet_name}")
            dfs.append(WorkbookUtils.read_workbook(loc, sheet_name))
        else:
            raise ValueError("Invalid file or directory path.")

        return pd.concat(dfs, axis=0, ignore_index=True)