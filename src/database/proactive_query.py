from src.lib.mysql import query


def get_pid_rollback(project_ids: list[str]):
    placeholders = ", ".join(["%s"] * len(project_ids))
    sql = f"""
        SELECT a.*
        FROM proactive2.t_project_rollback_detail a
        JOIN proactive2.projects b ON a.project_id = b.project_id
        WHERE a.cron_update IS NULL
        AND b.project_id IN ({placeholders})
    """
    return query(sql, tuple(project_ids))
