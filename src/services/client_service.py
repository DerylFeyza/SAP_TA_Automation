import win32com.client
from dotenv import load_dotenv
from pathlib import Path
import subprocess
import time
import os

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)


def checkGUIConnection():
    try:
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        if not SapGuiAuto:
            print("❌ SAP GUI is not running.")
            exit()

        application = SapGuiAuto.GetScriptingEngine
        if application is None or application.Children.Count == 0:
            print("⚠️ SAP GUI Scripting is enabled, but no connections are open.")
            print("👉 Open SAP Logon and connect to a system first.")
            exit()

        connection = application.Children(0)
        if connection.Children.Count == 0:
            print("⚠️ Connected to SAP system, but no active sessions found.")
            print("👉 Log in with your user credentials to start a session.")
            exit()

        session = connection.Children(0)
        print("✅ SAP GUI Scripting is ENABLED and you are connected.")
        print("Session Info:")
        print(f"System: {session.Info.SystemName}")
        print(f"Client: {session.Info.Client}")
        print(f"User: {session.Info.User or 'Not logged in'}")

    except Exception as e:
        print("❌ SAP GUI Scripting not available or disabled.")
        print("Error:", e)


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
