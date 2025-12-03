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

        try:
            session.findById("wnd[1]/usr/ctxtTCNT-PROF_DB").text = "000000000001"
            session.findById("wnd[1]").sendVKey(0)
        except:
            pass

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
                # First, determine the expected number of columns
                with open(file_path, "r", encoding="utf-8") as f:
                    # Skip first line
                    f.readline()
                    # Read header line
                    header = f.readline()
                    # Count expected columns
                    expected_columns = header.count("|") + 1

                # Custom parsing function to handle lines with extra separators
                def process_line(line):
                    fields = line.strip().split("|")
                    if len(fields) > expected_columns:
                        # Combine extra fields with the last expected field
                        fields[expected_columns - 1] = "|".join(
                            fields[expected_columns - 1 :]
                        )
                        fields = fields[:expected_columns]
                    return fields

                # Read the file line by line and process
                with open(file_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                # Extract header and data (skipping rows 0 and 2)
                header = [col.strip() for col in lines[1].split("|")]
                data = [process_line(line) for line in lines[3:]]

                # Create DataFrame from processed data
                df = pd.DataFrame(data, columns=header)

            except UnicodeDecodeError:
                # Do the same for latin1 encoding
                with open(file_path, "r", encoding="latin1") as f:
                    # Skip first line
                    f.readline()
                    # Read header line
                    header = f.readline()
                    # Count expected columns
                    expected_columns = header.count("|") + 1

                # Read the file line by line and process
                with open(file_path, "r", encoding="latin1") as f:
                    lines = f.readlines()

                # Extract header and data (skipping rows 0 and 2)
                header = [col.strip() for col in lines[1].split("|")]
                data = [process_line(line) for line in lines[3:]]

                # Create DataFrame from processed data
                df = pd.DataFrame(data, columns=header)

            df = df.loc[
                :, ~df.columns.str.contains("^Unnamed") & (df.columns.str.strip() != "")
            ]
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df = df[~df["Title"].isin(["", None])]
            
            def safe_convert_level(val):
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return None

            df["Level"] = df["Level"].apply(safe_convert_level)
            df = df[df["Level"].notna()]
            df = df[~df["Level"].isin([0, 1])]
            df["Level1"] = df["Title"].str[:10]
            df["Level2"] = df["Title"].str[:15]
            df["CurrentStatus"] = df["Status"].astype(str).str[-4:]

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


def start_necessary_session(connection, max_cluster):
    while connection.Children.Count < max_cluster:
        print(f"Current session count: {connection.Children.Count}")
        print(f"Current max cluster: {max_cluster}")
        session0 = connection.Children(0)
        session0.SendCommand("/oCNMASSSTATUS")
        time.sleep(1)
    print(f"Max cluster: {max_cluster}")


def execute_bast(status_dfs: dict, date_identifier):
    try:
        bast_df = status_dfs["BAST"]
        bast_df = bast_df.dropna(subset=["Cluster"])
        max_cluster = bast_df["Cluster"].max()
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.Children(0)

        start_necessary_session(connection, max_cluster)

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

        bulk_session_result = bulk_session_orchestrator(
            cluster_id, session_id, cluster_map, date_identifier, "BAST"
        )

        bast_df = bast_df.copy()
        bast_df["New User Status"] = bast_df["Title"].map(bulk_session_result["status"])
        status_dfs["BAST"] = bast_df
        bast_report_df = get_status_report(status_dfs["BAST"], "BNOV")
        return {
            "executed": bulk_session_result["executed"],
            "status": status_dfs,
            "report": bast_report_df,
        }
    except Exception as e:
        print(f"Error automating BAST: {e}")
        return None


def execute_cancel(status_dfs: dict, date_identifier):
    try:
        cancel_df = status_dfs["CANCEL"]
        cancel_df = cancel_df.dropna(subset=["Cluster"])
        max_cluster = cancel_df["Cluster"].max()

        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.Children(0)

        start_necessary_session(connection, max_cluster)

        cluster_map = {
            cluster: idx for idx, cluster in enumerate(cancel_df["Cluster"].unique())
        }

        for cluster_id, df_chunk in cancel_df.groupby("Cluster"):
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
            session.findById("wnd[0]/usr/chkUSR").selected = False
            session.findById("wnd[0]/usr/chkSYS").selected = True
            session.findById("wnd[0]/usr/txtCN_STUFE-LOW").text = "2"
            session.findById("wnd[0]/usr/cmbSSYS").key = "DLFL"
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

        bulk_session_result = bulk_session_orchestrator(
            cluster_id, session_id, cluster_map, date_identifier, "CANCEL"
        )

        cancel_df = cancel_df.copy()
        cancel_df["New User Status"] = cancel_df["Title"].map(
            bulk_session_result["status"]
        )
        status_dfs["CANCEL"] = cancel_df
        return {
            "executed": bulk_session_result["executed"],
            "status": status_dfs,
        }
    except Exception as e:
        print(f"Error automating CANCEL: {e}")
        return None


def execute_close(status_dfs: dict, date_identifier):
    try:
        close_df = status_dfs["CLOSE"]
        close_df = close_df.dropna(subset=["Cluster"])
        max_cluster = close_df["Cluster"].max()

        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.Children(0)

        start_necessary_session(connection, max_cluster)

        cluster_map = {
            cluster: idx for idx, cluster in enumerate(close_df["Cluster"].unique())
        }

        for cluster_id, df_chunk in close_df.groupby("Cluster"):
            session_id = cluster_map[cluster_id]
            print(f"Session ID: {session_id}")
            print(
                f"Processing cluster: {cluster_id}, session: {session_id}, rows: {len(df_chunk)}"
            )
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
            session.findById("wnd[0]/usr/chkSYS").selected = True
            session.findById("wnd[0]/usr/cmbSSYS").key = "TECO"
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
            session.findById("wnd[0]/usr/cmbUUSR").key = "CLNV"
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

        bulk_session_result = bulk_session_orchestrator(
            cluster_id, session_id, cluster_map, date_identifier, "CLOSE"
        )

        close_df = close_df.copy()
        close_df["New User Status"] = close_df["Title"].map(
            bulk_session_result["status"]
        )
        status_dfs["CLOSE"] = close_df
        bast_report_df = get_status_report(status_dfs["CLOSE"], "CLNV")

        return {
            "executed": bulk_session_result["executed"],
            "status": status_dfs,
            "report": bast_report_df,
        }
    except Exception as e:
        print(f"Error automating CLOSE: {e}")
        return None


def bulk_session_orchestrator(
    cluster_id, session_id, cluster_map, date_identifier, executed_status
):
    try:
        futures = []
        executed_results = []
        with ProcessPoolExecutor() as executor:
            for cluster_id, session_id in cluster_map.items():
                print(f"Submitting cluster {cluster_id} -> session {session_id}")
                futures.append(
                    executor.submit(
                        bulk_execute_session,
                        session_id,
                        executed_status,
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
                        executed_cluster = pd.read_csv(
                            full_path, sep="|", skipinitialspace=True, skiprows=[0, 2]
                        )
                        executed_cluster = executed_cluster.loc[
                            :, ~executed_cluster.columns.str.contains("^Unnamed")
                        ]
                        executed_cluster.columns = executed_cluster.columns.str.strip()
                        executed_cluster = executed_cluster.map(
                            lambda x: x.strip() if isinstance(x, str) else x
                        )
                        executed_cluster = executed_cluster.dropna(how="all")
                        executed_cluster["session_id"] = result["session"]
                        executed_cluster["cluster_id"] = result["cluster"]
                        executed_results.append(executed_cluster)
                        status_map.update(
                            dict(
                                zip(
                                    executed_cluster["Object Key"],
                                    executed_cluster["New User Status"],
                                )
                            )
                        )
                    except Exception as e:
                        print(f"Error reading {full_path}: {e}")

        executed_cluster = (
            pd.concat(executed_results, ignore_index=True)
            if executed_results
            else pd.DataFrame()
        )

        return {
            "executed": executed_cluster,
            "status": status_map,
        }
    except Exception as e:
        print(f"Error orchestrating bulk execute: {e}")
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
