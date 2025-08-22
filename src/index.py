# fastapi_server.py
from fastapi import FastAPI, File, UploadFile
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

app = FastAPI()

UPLOAD_FOLDER = r"./uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.post("/automate")
async def upload_excel(file: UploadFile = File(...)):
    file_extension = os.path.splitext(file.filename)[1]
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"automate_{current_date}{file_extension}"
    file_path = os.path.join(UPLOAD_FOLDER, new_filename)

    initializeSAPLogon()
    sapClient = win32com.client.GetObject("SAPGUI")
    checkLogin = checkGUIConnection(sapClient)
    if (
        checkLogin["status"] == "not logged in"
        or checkLogin["status"] == "no connection"
    ):
        loginConnection(sapClient)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Trigger automation (replace with your SAP script)
    # Example: subprocess.Popen(["python", "cn41_automation.py", file_path])
    print(f"Automation would run on: {file_path}")

    return {"status": "success", "file_saved": file_path}


@app.post("/test")
async def test_endpoint():
    results = get_pid_rollback(["W27-314/2023", "W28-111/2023"])
    return {"message": "This is a test endpoint", "data": results}
