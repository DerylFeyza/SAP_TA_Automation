import os
import pandas as pd
import pyperclip
import multiprocessing


def get_pid_sap(session, cleaned_df: pd.DataFrame, date_identifier):
    try:
        processed_statuses = cleaned_df["Status To Be"].unique()
        print(processed_statuses)
        status_dfs = {}
        for status in processed_statuses:
            print(f"Processing status: {status}")
            session.StartTransaction("CN41N")

            try:
                session.findById("wnd[1]/usr/ctxtTCNTT-PROFID").text = "000000000001"
                session.findById("wnd[1]").sendVKey(0)
            except:
                pass

            project_ids = (
                cleaned_df.loc[cleaned_df["Status To Be"] == status, "PROJECT_ID_SAP"]
                .astype(str)
                .tolist()
            )

            pyperclip.copy("\r\n".join(project_ids))
            session.findById("wnd[0]/usr/btn%_CN_PSPNR_%_APP_%-VALU_PUSH").press()
            session.findById("wnd[1]/tbar[0]/btn[16]").press()
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

            filename = f"{status}_{date_identifier}.txt"
            session.findById("wnd[1]/usr/ctxtDY_PATH").text = os.getenv(
                "SAP_OUTPUT_PATH"
            )
            session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename
            session.findById("wnd[1]/tbar[0]/btn[0]").press()

            file_path = os.path.join(os.getenv("SAP_OUTPUT_PATH"), filename)
            df = pd.read_csv(file_path, sep="|", skipinitialspace=True, skiprows=[0, 2])
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            df.columns = df.columns.str.strip()
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df = df[~df["Level"].isin([0, 1])]
            status_dfs[status] = df

        print(status_dfs)
        return status_dfs
    except Exception as e:
        print(f"Error automating: {e}")
    return None
