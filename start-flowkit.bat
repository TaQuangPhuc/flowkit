@echo off
REM Launch FlowKit agent bound to all interfaces so the VPS reaches it via
REM Tailscale (100.105.202.112). Loopback stays available for local callers.
REM secrets.env (gitignored) carries FLOWKIT_API_KEY for the /api/* remote gate.
cd /d "%~dp0"
set API_HOST=0.0.0.0
if exist secrets.env for /f "usebackq tokens=1,* delims==" %%a in ("secrets.env") do set %%a=%%b
"C:\Program Files\Python312\python.exe" -m agent.main
