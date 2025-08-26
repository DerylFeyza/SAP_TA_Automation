from src.database.proactive_query import get_pid_rollback, get_pid_report
import pandas as pd


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
            proactive_df[["project_id_sap", "project_id"]],
            left_on="Level2",
            right_on="project_id_sap",
            how="left",
        )

    if not rollback_df.empty and "project_id" in rollback_df.columns:
        print("rollback value exist")
        rollback_project_ids = rollback_df["project_id"].astype(str).tolist()
        print(rollback_project_ids)
        cleaned_rollback_df = df[
            ~df["PROJECT_ID"].astype(str).isin(rollback_project_ids)
        ]
        removed_rows = df[
            df["PROJECT_ID"].astype(str).str.strip().isin(rollback_project_ids)
        ]
        print("Removed rows:")
        print(removed_rows)
    else:
        cleaned_rollback_df = df

    return {"error": False, "rollback": rollback_df, "cleaned": cleaned_rollback_df}
