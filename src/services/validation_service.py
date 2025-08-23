from src.database.proactive_query import get_pid_rollback
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

    project_ids = df["PROJECT_ID_SAP"].dropna().astype(str).tolist()
    if not project_ids:
        print("No project IDs found in column B.")
        return {"error": True, "message": "No project IDs found."}

    results = get_pid_rollback(project_ids)
    if not results:
        print("No rollback details found for the given project IDs.")
        return {"error": False, "rollback": pd.DataFrame(), "cleaned": df}

    rollback_df = pd.DataFrame(results)

    if "project_id" in rollback_df.columns:
        rollback_project_ids = rollback_df["project_id"].astype(str).tolist()
        cleaned_rollback_df = df[
            ~df["PROJECT_ID_SAP"].astype(str).isin(rollback_project_ids)
        ]
    else:
        cleaned_rollback_df = df

    return {"error": False, "rollback": rollback_df, "cleaned": cleaned_rollback_df}
