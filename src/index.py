from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse
import shutil
import os
from datetime import datetime
from src.services.client_service import (
    getSession,
)
from src.database.proactive_query import get_pid_rollback
from src.services.validation_service import (
    validate_rollback,
    validate_cancel,
)
from src.services.automation_service import (
    get_pid_sap,
    execute_bast,
    execute_cancel,
    execute_close,
)
from src.services.format_service import clusterize_dfs
from io import BytesIO
import pandas as pd

app = FastAPI()

UPLOAD_FOLDER = r"./uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.post("/automate")
async def upload_excel(file: UploadFile = File(...)):
    file_extension = os.path.splitext(file.filename)[1]
    date_identifier = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"automate_{date_identifier}{file_extension}"
    file_path = os.path.join(UPLOAD_FOLDER, new_filename)
    session = getSession()["session"]

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    original_df = pd.read_excel(file_path, sheet_name="Format")

    validate_rollback_result = validate_rollback(original_df)
    if validate_rollback_result.get("error") == True:
        return {"error": True, "message": validate_rollback_result.get("message")}

    cleaned_df = validate_rollback_result["cleaned"]
    rollback_df = validate_rollback_result["rollback"]

    status_dfs = {}
    status_dfs = get_pid_sap(session, cleaned_df, date_identifier, status_dfs)
    if (
        status_dfs.get("CLOSE") is not None
        and not status_dfs.get("CLOSE").dropna(how="all").empty
    ):
        validated_cancel_res = validate_cancel(session, status_dfs, date_identifier)
        status_dfs = validated_cancel_res["status"]

    clustered_res = clusterize_dfs(status_dfs)
    status_dfs = clustered_res["status"]
    clustered_df = clustered_res["clustered"]

    bast_df = status_dfs.get("BAST")
    executed_bast_df = None
    report_bast_df = None
    if bast_df is not None and not bast_df.dropna(how="all").empty:
        result_executed_bast = execute_bast(status_dfs, date_identifier)
        executed_bast_df = result_executed_bast["executed"]
        report_bast_df = result_executed_bast["report"]
        status_dfs = result_executed_bast["status"]

    close_df = status_dfs.get("CLOSE")
    executed_close_df = None
    report_close_df = None
    if close_df is not None and not close_df.dropna(how="all").empty:
        result_executed_close = execute_close(status_dfs, date_identifier)
        executed_close_df = result_executed_close["executed"]
        report_close_df = result_executed_close["report"]
        status_dfs = result_executed_close["status"]

    cancel_df = status_dfs.get("CANCEL")
    executed_cancel_df = None
    if cancel_df is not None and not cancel_df.dropna(how="all").empty:
        result_executed_cancel = execute_cancel(status_dfs, date_identifier)
        executed_cancel_df = result_executed_cancel["executed"]
        status_dfs = result_executed_cancel["status"]

    draft = BytesIO()
    with pd.ExcelWriter(draft, engine="openpyxl") as writer:
        original_df.to_excel(writer, sheet_name="Format", index=False)
        cleaned_df.to_excel(writer, sheet_name="cleaned", index=False)
        if rollback_df is not None and not rollback_df.dropna(how="all").empty:
            rollback_df.to_excel(writer, sheet_name="rollback", index=False)
        for status, df in status_dfs.items():
            sheet_name = sheet_name = (
                str(status)[:31].replace("/", "_").replace("\\", "_")
            )
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        for status, df in clustered_df.items():
            sheet_name = (
                str(status)[:31].replace("/", "_").replace("\\", "_") + "_CLUSTERED"
            )
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        if (
            executed_bast_df is not None
            and not executed_bast_df.dropna(how="all").empty
        ):
            executed_bast_df.to_excel(writer, sheet_name="EXECUTED-BAST", index=False)
        if report_bast_df is not None and not report_bast_df.dropna(how="all").empty:
            report_bast_df.to_excel(writer, sheet_name="REPORT-BAST", index=False)

        if (
            executed_cancel_df is not None
            and not executed_cancel_df.dropna(how="all").empty
        ):
            executed_cancel_df.to_excel(
                writer, sheet_name="EXECUTED-CANCEL", index=False
            )

        if (
            executed_close_df is not None
            and not executed_close_df.dropna(how="all").empty
        ):
            executed_close_df.to_excel(writer, sheet_name="EXECUTED-CLOSE", index=False)
        if report_close_df is not None and not report_close_df.dropna(how="all").empty:
            report_close_df.to_excel(writer, sheet_name="REPORT-CLOSE", index=False)

        if (
            validated_cancel_res is not None
            and "reservation" in validated_cancel_res
            and validated_cancel_res["reservation"] is not None
            and not validated_cancel_res["reservation"].dropna(how="all").empty
        ):
            validated_cancel_res["reservation"].to_excel(
                writer, sheet_name="CANCEL-RESERVATION", index=False
            )
        if (
            validated_cancel_res is not None
            and "budgeting" in validated_cancel_res
            and validated_cancel_res["budgeting"] is not None
            and not validated_cancel_res["budgeting"].dropna(how="all").empty
        ):
            validated_cancel_res["budgeting"].to_excel(
                writer, sheet_name="CANCEL-BUDGETING", index=False
            )
        if (
            validated_cancel_res is not None
            and "excluded" in validated_cancel_res
            and validated_cancel_res["excluded"] is not None
            and not validated_cancel_res["excluded"].dropna(how="all").empty
        ):
            validated_cancel_res["excluded"].to_excel(
                writer, sheet_name="CANCEL-EXCLUDED", index=False
            )
        if (
            validated_cancel_res is not None
            and "accost" in validated_cancel_res
            and validated_cancel_res["accost"] is not None
            and not validated_cancel_res["accost"].dropna(how="all").empty
        ):
            validated_cancel_res["accost"].to_excel(
                writer, sheet_name="CANCEL-ACTUALCOST", index=False
            )

    draft.seek(0)
    return StreamingResponse(
        draft,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="processed_{date_identifier}.xlsx"'
        },
    )


@app.post("/clusterize")
async def clusterize(file: UploadFile = File(...)):
    file_extension = os.path.splitext(file.filename)[1]
    date_identifier = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"automate_{date_identifier}{file_extension}"
    file_path = os.path.join(UPLOAD_FOLDER, new_filename)
    session = getSession()["session"]

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    original_df = pd.read_excel(file_path, sheet_name="Format")

    validate_rollback_result = validate_rollback(original_df)
    if validate_rollback_result.get("error") == True:
        return {"error": True, "message": validate_rollback_result.get("message")}

    cleaned_df = validate_rollback_result["cleaned"]
    rollback_df = validate_rollback_result["rollback"]

    status_dfs = {}
    status_dfs = get_pid_sap(session, cleaned_df, date_identifier, status_dfs)
    clustered_res = clusterize_dfs(status_dfs)
    status_dfs = clustered_res["status"]
    clustered_df = clustered_res["clustered"]

    draft = BytesIO()
    with pd.ExcelWriter(draft, engine="openpyxl") as writer:
        original_df.to_excel(writer, sheet_name="Format", index=False)
        cleaned_df.to_excel(writer, sheet_name="cleaned", index=False)
        if not rollback_df.dropna(how="all").empty:
            rollback_df.to_excel(writer, sheet_name="rollback", index=False)
        for status, df in status_dfs.items():
            sheet_name = sheet_name = (
                str(status)[:31].replace("/", "_").replace("\\", "_")
            )
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        for status, df in clustered_df.items():
            sheet_name = (
                str(status)[:31].replace("/", "_").replace("\\", "_") + "_CLUSTERED"
            )
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    draft.seek(0)
    return StreamingResponse(
        draft,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="CLUSTERIZED_{date_identifier}.xlsx"'
        },
    )


@app.post("/validate_cancel")
async def validateCancel(file: UploadFile = File(...)):
    file_extension = os.path.splitext(file.filename)[1]
    date_identifier = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"VALIDATEDCANCEL_{date_identifier}{file_extension}"
    file_path = os.path.join(UPLOAD_FOLDER, new_filename)
    session = getSession()["session"]
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    original_df = pd.read_excel(file_path, sheet_name="Format")
    original_df = original_df[original_df["Status To Be"].str.upper() == "CANCEL"]
    validate_rollback_result = validate_rollback(original_df)
    if validate_rollback_result.get("error") == True:
        return {"error": True, "message": validate_rollback_result.get("message")}

    cleaned_df = validate_rollback_result["cleaned"]
    rollback_df = validate_rollback_result["rollback"]

    status_dfs = {}
    status_dfs = get_pid_sap(session, cleaned_df, date_identifier, status_dfs)
    validated_cancel_res = validate_cancel(session, status_dfs, date_identifier)
    clustered_res = clusterize_dfs(status_dfs)
    clustered_df = clustered_res["clustered"]
    status_dfs = clustered_res["status"]

    draft = BytesIO()
    status_dfs = validated_cancel_res["status"]

    with pd.ExcelWriter(draft, engine="openpyxl") as writer:
        original_df.to_excel(writer, sheet_name="Format", index=False)
        cleaned_df.to_excel(writer, sheet_name="cleaned", index=False)
        if not rollback_df.dropna(how="all").empty:
            rollback_df.to_excel(writer, sheet_name="rollback", index=False)

        if not status_dfs["CANCEL"].dropna(how="all").empty:
            status_dfs["CANCEL"].to_excel(writer, sheet_name="CANCEL", index=False)
        if not clustered_df["CANCEL"].dropna(how="all").empty:
            clustered_df["CANCEL"].to_excel(
                writer, sheet_name="CANCEL-CLUSTERED", index=False
            )
        if not validated_cancel_res["reservation"].dropna(how="all").empty:
            validated_cancel_res["reservation"].to_excel(
                writer, sheet_name="CANCEL-RESERVATION", index=False
            )
        if not validated_cancel_res["budgeting"].dropna(how="all").empty:
            validated_cancel_res["budgeting"].to_excel(
                writer, sheet_name="CANCEL-BUDGETING", index=False
            )
        if not validated_cancel_res["excluded"].dropna(how="all").empty:
            validated_cancel_res["excluded"].to_excel(
                writer, sheet_name="CANCEL-EXCLUDED", index=False
            )
        if not validated_cancel_res["accost"].dropna(how="all").empty:
            validated_cancel_res["accost"].to_excel(
                writer, sheet_name="CANCEL-ACTUALCOST", index=False
            )

    draft.seek(0)
    return StreamingResponse(
        draft,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="VALIDATEDCANCEL_{date_identifier}.xlsx"'
        },
    )


@app.post("/test")
async def test_endpoint():
    results = get_pid_rollback(["W27-314/2023", "W28-111/2023"])
    return {"message": "This is a test endpoint", "data": results}
