SET HADOOP_USER_NAME=mtpkrbrs

if not defined JAVA_HOME (
  echo JAVA_HOME is not defined use D:\data\java.latest
  set JAVA_HOME=D:\data\java.latest
)
if not defined HADOOP_CONF_DIR (
  echo HADOOP_CONF_DIR is not defined use D:\data\hadoop.config\latest
  set HADOOP_CONF_DIR=D:\data\hadoop.config\latest
)

if not defined HADOOP_HOME @(
  set HADOOP_HOME=D:\data\hadoop.latest
)

# Generate configs at first
powershell -NoProfile ./GenerateCelebornConfig.ps1

set targetdir=%DATADIR%\celeborn.latest

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

call sbin\\start-master.cmd