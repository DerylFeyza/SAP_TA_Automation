from src.database.proactive_query import (
    get_pid_rollback,
    get_pid_report,
    get_reservation,
)
import pandas as pd
import pyperclip
import os


def validate_rollback(df: pd.DataFrame):
    df["Status To Be"] = df["Status To Be"].str.upper()
    unique_status_values = df["Status To Be"].unique()
    valid_statuses = {"CANCEL", "CLOSE", "BAST"}
    invalid_statuses = [
        status for status in unique_status_values if status not in valid_statuses
    ]

    if invalid_statuses:
        return {
            "error": True,
            "message": f"Status must be either CANCEL, CLOSE, or BAST. Found invalid status: {', '.join(invalid_statuses)}",
        }

    project_ids = df["PROJECT_ID_SAP"].dropna().astype(str).str[:15].unique().tolist()
    if not project_ids:
        print("No project IDs found in column B.")
        return {"error": True, "message": "No project IDs found."}

    proactive_res = get_pid_report(project_ids)
    proactive_df = pd.DataFrame(proactive_res)
    proactive_ids = proactive_df["project_id"].dropna().astype(str).tolist()
    results = get_pid_rollback(proactive_ids)
    rollback_df = pd.DataFrame(results) if results else pd.DataFrame()

    print(proactive_df.head())
    if not proactive_df.empty and "project_id" in proactive_df.columns:
        df["Level2"] = df["PROJECT_ID_SAP"].str[:15]
        df = df.merge(
            proactive_df.rename(columns={"project_id": "project_id_db"})[
                ["project_id_sap", "project_id_db"]
            ],
            left_on="Level2",
            right_on="project_id_sap",
            how="left",
        )

    if not rollback_df.empty and "project_id" in rollback_df.columns:
        print("rollback value exist")
        rollback_project_ids = rollback_df["project_id"].astype(str).tolist()
        print(rollback_project_ids)
        cleaned_rollback_df = df[
            ~df["project_id_db"].astype(str).isin(rollback_project_ids)
        ]
        removed_rows = df[
            df["project_id_db"].astype(str).str.strip().isin(rollback_project_ids)
        ]
        print("Removed rows:")
        print(removed_rows)
    else:
        cleaned_rollback_df = df

    return {"error": False, "rollback": rollback_df, "cleaned": cleaned_rollback_df}


def validate_actual_cost(session, cancel_df: pd.DataFrame, date_identifier):
    try:
        session.StartTransaction("CN41N")

        try:
            session.findById("wnd[1]/usr/ctxtTCNTT-PROFID").text = "000000000001"
            session.findById("wnd[1]").sendVKey(0)
        except:
            pass

        project_ids = cancel_df["Level2"].dropna().astype(str).unique().tolist()
        pyperclip.copy("\r\n".join(project_ids))
        session.findById("wnd[0]/usr/btn%_CN_PSPNR_%_APP_%-VALU_PUSH").press()
        session.findById("wnd[1]/tbar[0]/btn[16]").press()
        session.findById("wnd[1]/tbar[0]/btn[24]").press()
        session.findById("wnd[1]/tbar[0]/btn[8]").press()

        session.findById("wnd[0]/usr/ctxtP_DISVAR").text = "/TA-1"
        session.findById("wnd[0]").sendVKey(8)

        session.findById(
            "wnd[0]/usr/cntlCUSTCONTAINER_ALV_TREE/shellcont/shell/shellcont[1]/shell[0]"
        ).pressContextButton("MENU_SAVE")
        session.findById(
            "wnd[0]/usr/cntlCUSTCONTAINER_ALV_TREE/shellcont/shell/shellcont[1]/shell[0]"
        ).selectContextMenuItem("%PC")

        filename = f"CANCELVALIDATION_{date_identifier}.txt"
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = os.getenv("SAP_OUTPUT_PATH")
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

        numeric_columns = [
            "Proj.cost plan",
            "Budget",
            "Release",
            "Act.costs",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .replace("", "0"),
                    errors="coerce",
                )

        return df
    except Exception as e:
        print(f"Error automating: {e}")
        return pd.DataFrame()


def validate_has_reservation(cleaned_df: pd.DataFrame):
    try:
        project_ids = cleaned_df["project_id_db"].dropna().astype(str).unique().tolist()
        reservation_res = get_reservation(project_ids)
        reservation_df = pd.DataFrame(reservation_res)
        return reservation_df
    except Exception as e:
        print(f"Error validating has reservation: {e}")
        return pd.DataFrame()


def validate_check_budgeting(session, cleaned_df: pd.DataFrame, date_identifier):
    try:
        session.StartTransaction("ZPS004")
        project_id_saps = cleaned_df["Level2"].dropna().astype(str).unique().tolist()
        project_id_saps = [f"{pid}*" for pid in project_id_saps]

        pyperclip.copy("\r\n".join(project_id_saps))
        session.findById("wnd[0]/usr/btn%_SP$00001_%_APP_%-VALU_PUSH").press()
        session.findById("wnd[1]/tbar[0]/btn[16]").press()
        session.findById("wnd[1]/tbar[0]/btn[24]").press()
        session.findById("wnd[1]/tbar[0]/btn[8]").press()
        session.findById("wnd[0]").sendVKey(8)

        filename = f"BUDGETING{date_identifier}.txt"
        session.findById(
            "wnd[0]/usr/cntlCONTAINER/shellcont/shell"
        ).pressToolbarContextButton("&MB_EXPORT")
        session.findById(
            "wnd[0]/usr/cntlCONTAINER/shellcont/shell"
        ).selectContextMenuItem("&PC")
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = os.getenv("SAP_OUTPUT_PATH")
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = filename
        session.findById("wnd[1]/tbar[0]/btn[0]").press()

        file_path = os.path.join(os.getenv("SAP_OUTPUT_PATH"), filename)

        try:
            df = pd.read_csv(
                file_path,
                sep="|",
                skiprows=[0, 1, 2, 3, 5],
                encoding="utf-8",
            )
        except UnicodeDecodeError:
            df = pd.read_csv(
                file_path,
                sep="|",
                skiprows=[0, 1, 2, 3, 5],
                encoding="latin1",
            )

        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        df.columns = df.columns.str.strip()
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df = df[~df.iloc[:, 0].astype(str).str.contains(r"^-+$|^\*$", na=False)]
        df = df.dropna(how="all")

        return df
    except Exception as e:
        print(f"Error validating has budgeting: {e}")
        return pd.DataFrame()


def exclude_cancel_validated(
    cancel_df: pd.DataFrame,
    accost_df: pd.DataFrame,
    reservation_df: pd.DataFrame,
    budgeting_df: pd.DataFrame,
):
    try:
        reservation_project_ids = reservation_df["project_id"].astype(str).tolist()
        accost_project_ids = (
            accost_df[
                (accost_df["Title"].str.len() == 15) & (accost_df["Act.costs"] > 0)
            ]["Title"]
            .astype(str)
            .tolist()
        )
        budgeting_project_ids = (
            budgeting_df[
                (budgeting_df["Description"] == "Budgeting")
                & (budgeting_df["Available Budget Original"].notna())
            ]["WBS element"]
            .astype(str)
            .str[:15]
            .tolist()
        )
        cancel_df = cancel_df[
            ~cancel_df["project_id_db"].astype(str).isin(reservation_project_ids)
        ]
        cancel_df = cancel_df[~cancel_df["Level2"].astype(str).isin(accost_project_ids)]
        cancel_df = cancel_df[
            ~cancel_df["Level2"].astype(str).isin(budgeting_project_ids)
        ]

        return cancel_df
    except Exception as e:
        print(f"Error excluding validated cancellations: {e}")
        return cancel_df
