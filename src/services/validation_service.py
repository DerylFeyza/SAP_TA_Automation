from src.database.proactive_query import get_pid_rollback
import pandas as pd


def validate_rollback(df: pd.DataFrame):
    project_ids = df["PROJECT_ID_SAP"].dropna().astype(str).tolist()
    if not project_ids:
        print("No project IDs found in column B.")
        return []

    results = get_pid_rollback(project_ids)
    if not results:
        print("No rollback details found for the given project IDs.")
        return []

    rollback_df = pd.DataFrame(results)

    if "project_id" in rollback_df.columns:
        rollback_project_ids = rollback_df["project_id"].astype(str).tolist()
        cleaned_rollback_df = df[
            ~df["PROJECT_ID_SAP"].astype(str).isin(rollback_project_ids)
        ]
    else:
        cleaned_rollback_df = df

    return {"rollback": rollback_df, "cleaned": cleaned_rollback_df}
