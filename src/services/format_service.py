import pandas as pd
from src.database.proactive_query import get_pid_report
from fastapi import HTTPException


def clusterize_dfs(dfs: dict):
    try:
        clustered_dfs = {}
        print("clustering dfs", dfs.keys())
        for status, df in dfs.items():
            df = df.copy()

            if status.upper() == "CANCEL":
                dfs[status] = df
                continue

            if status.upper() == "CLOSE":
                df_included = df[df["CurrentStatus"] != "CLNV"].copy()
            elif status.upper() == "BAST":
                df_included = df[df["CurrentStatus"] != "BNOV"].copy()
            else:
                df_included = df

            level1_counts = df_included["Level1"].value_counts().reset_index()
            level1_counts.columns = ["Level1", "Count"]

            level1_counts = level1_counts.sort_values(
                by="Count", ascending=False
            ).reset_index(drop=True)

            num_clusters = min(12, len(level1_counts))

            if num_clusters > 0:
                cluster_totals = [0] * num_clusters
                clusters = [0] * len(level1_counts)

                for idx, count in enumerate(level1_counts["Count"]):
                    min_cluster = cluster_totals.index(min(cluster_totals))
                    clusters[idx] = min_cluster + 1
                    cluster_totals[min_cluster] += count

                level1_counts["Cluster"] = clusters
                level1_to_cluster = dict(
                    zip(level1_counts["Level1"], level1_counts["Cluster"])
                )

                df["Cluster"] = df["Level1"].map(level1_to_cluster)

                if status.upper() == "CLOSE":
                    df.loc[df["CurrentStatus"] == "CLNV", "Cluster"] = None
                elif status.upper() == "BAST":
                    df.loc[df["CurrentStatus"] == "BNOV", "Cluster"] = None

            clustered_dfs[status] = level1_counts
            dfs[status] = df

        return {"clustered": clustered_dfs, "status": dfs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def get_status_report(df: pd.DataFrame, updated_status):
    print(df)
    try:
        level2_list = (
            df[df["New User Status"] == updated_status]["Level2"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        results = get_pid_report(level2_list)
        report_df = pd.DataFrame(results)
        return report_df
    except Exception as e:
        print(f"Error generating status report: {e}")
        return pd.DataFrame()
