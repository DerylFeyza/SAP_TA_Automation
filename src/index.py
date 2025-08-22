# fastapi_server.py
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
from src.services.automation_service import get_pid_sap
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
    session = None
    initializeSAPLogon()
    sapClient = win32com.client.GetObject("SAPGUI")
    checkLogin = checkGUIConnection(sapClient)
    if (
        checkLogin["status"] == "not logged in"
        or checkLogin["status"] == "no connection"
    ):
        session = loginConnection(sapClient)["session"]
    else:
        session = checkLogin["session"]

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    original_df = pd.read_excel(file_path, sheet_name="Format")

    validate_rollback_result = validate_rollback(original_df)
    rollback_df = validate_rollback_result["rollback"]
    cleaned_df = validate_rollback_result["cleaned"]

    bast = get_pid_sap(session, cleaned_df, date_identifier)

    print(rollback_df.shape)
    draft = BytesIO()
    with pd.ExcelWriter(draft, engine="openpyxl") as writer:
        original_df.to_excel(writer, sheet_name="Format", index=False)
        rollback_df.to_excel(writer, sheet_name="rollback", index=False)
        bast.to_excel(writer, sheet_name="bast", index=False)

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
