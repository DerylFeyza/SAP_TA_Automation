import win32com.client
from dotenv import load_dotenv
from pathlib import Path
import subprocess
import time
import os
import pandas as pd
import pyperclip
from openpyxl import load_workbook


def get_pid_sap(session, cleaned_df: pd.DataFrame, date_identifier):
    try:
        # start the transaction
        session.StartTransaction("CN41N")
        session.findById("wnd[1]/usr/ctxtTCNTT-PROFID").text = "000000000001"
        session.findById("wnd[1]").sendVKey(0)

        project_ids = cleaned_df["PROJECT_ID_SAP"].astype(str).tolist()
        pyperclip.copy("\r\n".join(project_ids))
        session.findById("wnd[0]/usr/btn%_CN_PSPNR_%_APP_%-VALU_PUSH").press()
        session.findById("wnd[1]/tbar[0]/btn[24]").press()
        session.findById("wnd[1]/tbar[0]/btn[8]").press()
        session.findById("wnd[0]/usr/ctxtP_DISVAR").text = "/TA-STATUS"
        session.findById("wnd[0]").sendVKey(8)
        session.findById(
            "wnd[0]/usr/cntlCUSTCONTAINER_ALV_TREE/shellcont/shell/shellcont[1]/shell[0]"
        ).pressContextButton("MENU_SAVE")
        session.findById(
            "wnd[0]/usr/cntlCUSTCONTAINER_ALV_TREE/shellcont/shell/shellcont[1]/shell[0]"
        ).selectContextMenuItem("%PC")
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = os.getenv("SAP_OUTPUT_PATH")
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = (
            f"BAST_{date_identifier}.txt"
        )
        session.findById("wnd[1]/tbar[0]/btn[0]").press()

        file_path = os.path.join(
            os.getenv("SAP_OUTPUT_PATH"), f"BAST_{date_identifier}.txt"
        )
        bast_df = pd.read_csv(
            file_path, sep="|", skipinitialspace=True, skiprows=[0, 2]
        )
        bast_df = bast_df.loc[:, ~bast_df.columns.str.contains("^Unnamed")]
        bast_df.columns = bast_df.columns.str.strip()
        bast_df = bast_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        print(bast_df.head())
        return bast_df
    except Exception as e:
        print(f"Error automating: {e}")
    return None
