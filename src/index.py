# fastapi_server.py
from fastapi import FastAPI, File, UploadFile
import shutil
import os
from src.services.client_service import initializeSAPLogon

app = FastAPI()

UPLOAD_FOLDER = r"./uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.post("/automate")
async def upload_excel(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    initializeSAPLogon()
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Trigger automation (replace with your SAP script)
    # Example: subprocess.Popen(["python", "cn41_automation.py", file_path])
    print(f"Automation would run on: {file_path}")

    return {"status": "success", "file_saved": file_path}
