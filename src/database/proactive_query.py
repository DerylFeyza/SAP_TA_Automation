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


def get_pid_report(level2: list[str]):
    placeholders = ", ".join(["%s"] * len(level2))
    sql = f"""
        SELECT a.project_id, a.project_id_sap, b.phase_name, b.phase_id, c.status, a.current_user_status_sap
        FROM proactive2.projects a
        JOIN proactive2.phase b ON a.phase_id = b.phase_id
        JOIN proactive2.project_status c ON c.project_status_id = a.project_status_id
        WHERE a.project_id_sap IN ({placeholders})
    """
    return query(sql, tuple(level2))


def update_status_proactive(project_ids: list[str]):
    sql = f"""
        UPDATE proactive2.projects a
        SET a.project_status_id = 6, a.current_user_status_sap = 'BNOV', a.phase_id = '4'
        WHERE a.project_id_sap IN ({", ".join(["%s"] * len(project_ids))})
    """
    return query(sql, tuple(project_ids))
