@echo off

set targetdir=%DATADIR%\celeborn.latest

IF EXIST "%targetdir%" (
    rem First try to remove installed.flag as fast as possible to minimize chance that dependency serviec will detect old installed.flag
    rem Folder itself will be deleted below
    del "%targetdir%"\installed.flag
)

rem Generate configs at first
powershell -NoProfile ./GenerateCelebornConfig.ps1

rem Add a flag file to indicate the installation is done
set "CELEBORN_INSTALLED=%~dp0installed.flag"

rem Get current date and time
set "curTime=%date% %time%"

rem Write current time to the flag file
echo %curTime% > "%CELEBORN_INSTALLED%"

rem
rem Create symlink targettempdir to current folder
rem
set targettempdir=%targetdir%.temp
set waitInterval=10
:makelink
IF EXIST "%targettempdir%" (
    rd /s /q "%targettempdir%"
)

rem %cd% points to current folder
mklink /d "%targettempdir%" "%cd%"

if errorlevel 1 (
  echo [%date% %time%] Failed to create symbolic link at %targettempdir%, retrying after %waitInterval% seconds
  sleep %waitInterval%
  goto :makelink
)

echo [%date% %time%] Succeeded creating symbolic link at %targettempdir%

rem
rem Copy symlink targettempdir to overwrite symlink targetdir atomically
rem
xcopy /b /i "%targettempdir%" "%targetdir%"
if errorlevel 1 (
  echo [%date% %time%] Failed to copy symbolic link from %targettempdir% to %targetdir%
  exit /b 1
)

echo [%date% %time%] Succeeded copying symbolic link from %targettempdir% to %targetdir%
rem besteffort to remove
rd /s /q "%targettempdir%"

if not defined CELEBORN_HOME (
  echo CELEBORN_HOME is not defined use D:\data\celeborn.latest
  set CELEBORN_HOME=D:\data\celeborn.latest
)

rem Check if the flag file exists in CELEBORN_HOME
if not exist "%CELEBORN_HOME%\installed.flag" (
    echo Error: you are not current celeborn service or latest link not work,
    exiting now...
    exit /b 1
)

echo [%date% %time%] Succeeded setting up Celeborn
sleep 86400
exit /b 0
