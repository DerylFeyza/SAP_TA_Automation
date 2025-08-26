import os
import time
import pandas as pd
import pyperclip
import pythoncom
import win32com.client
from concurrent.futures import ProcessPoolExecutor, as_completed
from src.services.format_service import get_status_report


def get_pid_sap(session, cleaned_df: pd.DataFrame, date_identifier, status_dfs: dict):
    try:
        processed_statuses = cleaned_df["Status To Be"].unique()
        print(processed_statuses)
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

            try:
                df = pd.read_csv(
                    file_path,
                    sep="|",
                    skipinitialspace=True,
                    skiprows=[0, 2],
                    encoding="utf-8",
                )
            except UnicodeDecodeError:
                df = pd.read_csv(
                    file_path,
                    sep="|",
                    skipinitialspace=True,
                    skiprows=[0, 2],
                    encoding="latin1",
                )

            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df = df[~df["Level"].isin([0, 1])]
            df["Level1"] = df["Title"].str[:10]
            df["Level2"] = df["Title"].str[:15]

            mapping_df = cleaned_df[
                ["project_id_sap", "project_id_db"]
            ].drop_duplicates()
            df = df.merge(
                mapping_df,
                left_on="Level2",
                right_on="project_id_sap",
                how="left",
            )
            df = df.drop(columns=["project_id_sap"])
            status_dfs[status] = df
        return status_dfs
    except Exception as e:
        print(f"Error automating: {e}")
        return None


def execute_bast(status_dfs: pd.DataFrame, date_identifier):
    try:
        print("executing")
        bast_df = status_dfs["BAST"]
        bast_df = bast_df.dropna(subset=["Cluster"])
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
            session.findById("wnd[0]/usr/ctxtPROF").text = "ZTA01"
            session.findById("wnd[0]/usr/cmbUUSR").key = "BNOV"
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

        futures = []
        executed_results = []
        with ProcessPoolExecutor() as executor:
            for cluster_id, session_id in cluster_map.items():
                print(f"Submitting cluster {cluster_id} -> session {session_id}")
                futures.append(
                    executor.submit(
                        bulk_execute_session,
                        session_id,
                        "BAST",
                        cluster_id,
                        date_identifier,
                    )
                )

            status_map = {}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    full_path = os.path.join(
                        os.getenv("SAP_OUTPUT_PATH"), result["file"]
                    )
                    print(f"Reading file: {full_path}")
                    try:
                        executed_bast = pd.read_csv(
                            full_path, sep="|", skipinitialspace=True, skiprows=[0, 2]
                        )
                        executed_bast = executed_bast.loc[
                            :, ~executed_bast.columns.str.contains("^Unnamed")
                        ]
                        executed_bast.columns = executed_bast.columns.str.strip()
                        executed_bast = executed_bast.map(
                            lambda x: x.strip() if isinstance(x, str) else x
                        )
                        executed_bast = executed_bast.dropna(how="all")
                        executed_bast["session_id"] = result["session"]
                        executed_bast["cluster_id"] = result["cluster"]
                        executed_results.append(executed_bast)
                        status_map.update(
                            dict(
                                zip(
                                    executed_bast["Object Key"],
                                    executed_bast["New User Status"],
                                )
                            )
                        )
                    except Exception as e:
                        print(f"Error reading {full_path}: {e}")

        executed_bast = (
            pd.concat(executed_results, ignore_index=True)
            if executed_results
            else pd.DataFrame()
        )
        bast_df = bast_df.copy()
        bast_df["New User Status"] = bast_df["Title"].map(status_map)
        status_dfs["BAST"] = bast_df
        bast_report_df = get_status_report(status_dfs["BAST"], "BNOV")
        return {
            "executed": executed_bast,
            "status": status_dfs,
            "report": bast_report_df,
        }
    except Exception as e:
        print(f"Error automating: {e}")
        return None


def bulk_execute_session(session_id, status, cluster, date_identifier):
    pythoncom.CoInitialize()
    SapGuiAuto = win32com.client.GetObject("SAPGUI")
    application = SapGuiAuto.GetScriptingEngine
    connection = application.Children(0)
    session = connection.Children(session_id)

    try:
        print(f"Starting session {session_id} for cluster {cluster}")
        session.findById("wnd[0]/tbar[1]/btn[8]").press()
        session.findById("wnd[0]/tbar[1]/btn[45]").press()
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = os.getenv("SAP_OUTPUT_PATH")
        filename = f"{status}C{cluster}S{session_id}_{date_identifier}.txt"
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        print(f"Session {session_id} finished successfully")
        return {"file": filename, "session": session_id, "cluster": cluster}
    except Exception as e:
        print(f"Error in session {session_id}: {e}")
    finally:
        pythoncom.CoUninitialize()
