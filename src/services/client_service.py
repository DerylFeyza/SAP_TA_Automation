import win32com.client
from dotenv import load_dotenv
from pathlib import Path
import subprocess
import time
import os

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)


def wait_for_logon_window(timeout=20):
    SapGuiAuto = win32com.client.GetObject("SAPGUI")
    app = SapGuiAuto.GetScriptingEngine

    start = time.time()
    while time.time() - start < timeout:
        if app.Children.Count > 0:
            wnd = app.Children(0)
            if wnd.Children.Count > 0:
                return wnd.Children(0)
        time.sleep(1)

    raise TimeoutError("SAP Logon window did not appear in time")


def checkGUIConnection(sapGUIClient):
    try:

        application = sapGUIClient.GetScriptingEngine
        if application is None or application.Children.Count == 0:
            print("⚠️ SAP GUI Scripting is enabled, but no connections are open.")
            print("👉 Open SAP Logon and connect to a system first.")
            return {"status": "no connection"}

        connection = application.Children(0)
        if connection.Children.Count == 0:
            print("⚠️ Connected to SAP system, but no active sessions found.")
            print("👉 Log in with your user credentials to start a session.")
            return {"status": "not logged in"}

        session = connection.Children(0)
        print("✅ SAP GUI Scripting is ENABLED and you are connected.")
        print("Session Info:")
        print(f"System: {session.Info.SystemName}")
        print(f"Client: {session.Info.Client}")
        print(f"User: {session.Info.User or 'Not logged in'}")
        return {"status": "connected", "active": True, "session": session}

    except Exception as e:
        print("❌ SAP GUI Scripting not available or disabled.")
        print("Error:", e)
        return {"status": "error", "message": str(e)}


def initializeSAPLogon():
    try:
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
    except Exception:
        # SAP Logon is not running, start it
        subprocess.Popen(os.getenv("SAP_LOGON_PATH"))
        time.sleep(10)  # wait for SAP Logon to load
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine

    return application


def loginConnection(sapGUIClient):
    try:
        print("Loggin in...")
        sapGUIClient = win32com.client.GetObject("SAPGUI")
        application = sapGUIClient.GetScriptingEngine
        connection = application.OpenConnection(os.getenv("SAP_CONN_NAME"), True)
        session = connection.Children(0)
        session.findById("wnd[0]/usr/txtRSYST-BNAME").text = os.getenv("SAP_CONN_USER")
        session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = os.getenv(
            "SAP_CONN_PASSWORD"
        )
        session.findById("wnd[0]/usr/txtRSYST-MANDT").text = os.getenv(
            "SAP_CONN_CLIENT"
        )
        session.findById("wnd[0]/tbar[0]/btn[0]").press()
        popup = session.findById("wnd[1]", False)

        if popup:
            text = popup.text
            print("⚠️ SAP Multiple Logon Popup detected:", text)
            return {
                "status": "error",
                "error": "MTPLG",
                "message": "Multiple logon detected. Please resolve manually.",
            }

        session.findById("wnd[0]/tbar[0]/okcd").text = "/n"
        session.findById("wnd[0]").sendVKey(0)
        return {"status": "connected", "active": True, "session": session}
    except Exception as e:
        print("❌ Error logging into SAP.")
        print("Error:", e)
        return {
            "status": "error",
            "message": "Login Unsuccessful",
        }
