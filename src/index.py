from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse
import win32com.client
import shutil
import os
from datetime import datetime
from src.services.client_service import (
    initializeSAPLogon,
    checkGUIConnection,
    loginConnection,
)
from src.database.proactive_query import get_pid_rollback
from src.services.validation_service import validate_rollback
from src.services.automation_service import get_pid_sap, execute_bast
from src.services.format_service import clusterize_dfs, get_status_report
from io import BytesIO
import pandas as pd
from fastapi import HTTPException

app = FastAPI()

UPLOAD_FOLDER = r"./uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.post("/automate")
async def upload_excel(file: UploadFile = File(...)):
    file_extension = os.path.splitext(file.filename)[1]
    date_identifier = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"automate_{date_identifier}{file_extension}"
    file_path = os.path.join(UPLOAD_FOLDER, new_filename)
    session = None
    initializeSAPLogon()
    sapClient = win32com.client.GetObject("SAPGUI")
    checkLogin = checkGUIConnection(sapClient)
    if (
        checkLogin["status"] == "not logged in"
        or checkLogin["status"] == "no connection"
    ):
        loginResult = loginConnection(sapClient)
        if loginResult["status"] == "error":
            raise HTTPException(
                status_code=500, detail=loginResult.get("message", "SAP login failed")
            )
        session = loginResult["session"]
    else:
        session = checkLogin["session"]

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

    result_executed = execute_bast(status_dfs, date_identifier)
    executed_bast_df = result_executed["executed"]
    status_dfs = result_executed["status"]
    bast_report_df = get_status_report(status_dfs["BAST"], "BNOV")
    draft = BytesIO()
    with pd.ExcelWriter(draft, engine="openpyxl") as writer:
        original_df.to_excel(writer, sheet_name="Format", index=False)
        cleaned_df.to_excel(writer, sheet_name="cleaned", index=False)
        if not rollback_df.empty:
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
        if not executed_bast_df.empty:
            executed_bast_df.to_excel(writer, sheet_name="EXECUTED-BAST", index=False)
        if not bast_report_df.empty:
            bast_report_df.to_excel(writer, sheet_name="REPORT-BAST", index=False)

    draft.seek(0)
    return StreamingResponse(
        draft,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="processed_{date_identifier}.xlsx"'
        },
    )


@app.post("/test")
async def test_endpoint():
    results = get_pid_rollback(["W27-314/2023", "W28-111/2023"])
    return {"message": "This is a test endpoint", "data": results}
