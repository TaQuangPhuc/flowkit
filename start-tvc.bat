@echo off
REM Launch the Auto-TVC daemon (:8089). Remote callers must present X-TVC-Key
REM (TVC_API_KEY) and creates require a billed marker / run_token signed with
REM TVC_RUN_SECRET. secrets.env is gitignored — keep it off the repo.
cd /d "%~dp0"
if exist secrets.env for /f "usebackq tokens=1,* delims==" %%a in ("secrets.env") do set %%a=%%b
"C:\Program Files\Python312\python.exe" auto_tvc_server.py
