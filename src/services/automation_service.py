import os
import time
import pandas as pd
import pyperclip
import multiprocessing as mp
import pythoncom
import win32com.client


def get_pid_sap(session, cleaned_df: pd.DataFrame, date_identifier):
    try:
        processed_statuses = cleaned_df["Status To Be"].unique()
        print(processed_statuses)
        status_dfs = {}

        session.StartTransaction("CNMASSSTATUS")
        session.findById("wnd[1]/usr/ctxtTCNT-PROF_DB").text = "000000000001"
        session.findById("wnd[1]").sendVKey(0)

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
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df = df[~df["Level"].isin([0, 1])]
            status_dfs[status] = df

        print(status_dfs)
        return status_dfs
    except Exception as e:
        print(f"Error automating: {e}")
        return None


def execute_bast(clusterized_dfs: pd.DataFrame):
    try:
        print("executing")
        bast_df = clusterized_dfs["BAST"]
        max_cluster = bast_df["Cluster"].max()

        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.Children(0)

        while connection.Children.Count < max_cluster:
            print(f"Current session count: {connection.Children.Count}")
            print(f"Current max cluster: {max_cluster}")
            session0 = connection.Children(0)
            session0.SendCommand("/oCNMASSSTATUS")
            time.sleep(1)
        print(f"Max cluster: {max_cluster}")

        jobs = []
        cluster_map = {
            cluster: idx for idx, cluster in enumerate(bast_df["Cluster"].unique())
        }
        for cluster_id, df_chunk in bast_df.groupby("Cluster"):
            session_id = cluster_map[cluster_id]
            print(f"Session ID: {session_id}")
            print(f"Processing cluster: {cluster_id}, session: {session_id}")
            session = connection.Children(session_id)
            session.findById("wnd[0]").maximize()
            session.findById("wnd[0]").sendVKey(0)
            session.StartTransaction("CNMASSSTATUS")
            project_ids = df_chunk["Title"].astype(str).tolist()
            pyperclip.copy("\r\n".join(project_ids))
            session.findById("wnd[0]/usr/btn%_CN_PSPNR_%_APP_%-VALU_PUSH").press()
            session.findById("wnd[1]/tbar[0]/btn[16]").press()
            session.findById("wnd[1]/tbar[0]/btn[24]").press()
            session.findById("wnd[1]/tbar[0]/btn[8]").press()
            session.findById("wnd[0]/usr/chkUSR").selected = True
            session.findById("wnd[0]/usr/chkSYS").selected = False
            session.findById("wnd[0]/usr/txtCN_STUFE-LOW").text = "2"
            session.findById("wnd[0]/usr/ctxtPROF").setFocus()
            session.findById("wnd[0]/usr/ctxtPROF").caretPosition = 0
            session.findById("wnd[0]").sendVKey(4)
            session.findById(
                "wnd[1]/usr/sub/1[0,0]/sub/1/2[0,0]/sub/1/2/7[0,7]/lbl[1,7]"
            ).setFocus()
            session.findById(
                "wnd[1]/usr/sub/1[0,0]/sub/1/2[0,0]/sub/1/2/7[0,7]/lbl[1,7]"
            ).caretPosition = 6
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[0]/usr/cmbUUSR").key = "BNOV"
            # p = mp.Process(target=attach_session, args=(session_id, df_chunk, project_ids))
            session.findById("wnd[0]/usr/chkTEST").selected = False
            session.findById("wnd[0]").sendVKey(8)
            session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").setCurrentCell(
                -1, "WBS"
            )
            session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectColumn("WBS")
            session.findById("wnd[0]/tbar[1]/btn[29]").press()
            session.findById(
                "wnd[1]/usr/ssub%_SUBSCREEN_FREESEL:SAPLSSEL:1105/btn%_%%DYN001_%_APP_%-VALU_PUSH"
            ).press()
            session.findById("wnd[2]/tbar[0]/btn[16]").press()
            session.findById("wnd[2]/tbar[0]/btn[24]").press()
            session.findById("wnd[2]/tbar[0]/btn[8]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectAll()
            # p.start()
            # jobs.append(p)

        # for j in jobs:
        #     j.join()
        print("presiden goblok")
    except Exception as e:
        print(f"Error automating: {e}")
        return None


def bulk_execute_session(session_id):
    """Worker that runs in its own process"""
    pythoncom.CoInitialize()
    SapGuiAuto = win32com.client.GetObject("SAPGUI")
    application = SapGuiAuto.GetScriptingEngine
    connection = application.Children(0)
    session = connection.Children(session_id)

    try:
        session.findById("wnd[0]/tbar[1]/btn[8]").press()

    except Exception as e:
        print(f"Error in session {session_id}: {e}")
    finally:
        pythoncom.CoUninitialize()
