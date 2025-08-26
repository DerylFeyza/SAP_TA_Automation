import pandas as pd
from src.database.proactive_query import get_pid_report


def clusterize_dfs(dfs: dict):
    clustered_dfs = {}
    for status, df in dfs.items():
        df["Level1"] = df["Title"].str[:10]
        df["Level2"] = df["Title"].str[:15]
        level1_counts = df["Level1"].value_counts().reset_index()
        level1_counts.columns = ["Level1", "Count"]

        level1_counts = level1_counts.sort_values(
            by="Count", ascending=False
        ).reset_index(drop=True)

        num_clusters = min(6, len(level1_counts))
        total_count = level1_counts["Count"].sum()

        # if total_count < 1000:
        #     num_clusters = 1

        if num_clusters > 0:
            cluster_totals = [0] * num_clusters
            clusters = [0] * len(level1_counts)

            # Assign each row to the cluster with the lowest total count
            for idx, count in enumerate(level1_counts["Count"]):
                min_cluster = cluster_totals.index(min(cluster_totals))
                clusters[idx] = min_cluster + 1  # Cluster numbers start from 1
                cluster_totals[min_cluster] += count

            level1_counts["Cluster"] = clusters
            level1_to_cluster = dict(
                zip(level1_counts["Level1"], level1_counts["Cluster"])
            )
            df["Cluster"] = df["Level1"].map(level1_to_cluster)

        clustered_dfs[status] = level1_counts

    return {"clustered": clustered_dfs, "status": dfs}


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
