@echo off
setlocal enabledelayedexpansion

:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::    http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

:: Starts the celeborn master and workers on the machine this script is executed on.

if not defined CELEBORN_HOME (
    pushd "%~dp0.."
    set "CELEBORN_HOME=%CD%"
    popd
)

call "%CELEBORN_HOME%\sbin\load-celeborn-env.cmd"

set "HOSTS_FILE=%CELEBORN_CONF_DIR%\hosts"
set "DEFAULT_HOSTS=[master] localhost[worker] localhost"

:: Read hosts file or use default
if exist "%HOSTS_FILE%" (
    set "HOST_LIST="
    for /f "tokens=*" %%a in ('type "%HOSTS_FILE%"') do (
        set "LINE=%%a"
        if "!LINE:~0,1!"=="[" (
            set "PREFIX=!LINE!"
        ) else if not "!LINE!"=="" (
            set "HOST_LIST=!HOST_LIST!!PREFIX! !LINE!^"
        )
    )
) else (
    set "HOST_LIST=%DEFAULT_HOSTS%"
)

:: Start masters
for %%h in ("%HOST_LIST:^=";"%") do (
    set "LINE=%%~h"
    if "!LINE!"=="" goto :skip_master
    echo !LINE! | findstr /i "\[master\]" >nul
    if not errorlevel 1 (
        for /f "tokens=2" %%i in ("!LINE!") do (
            if "%%i"=="localhost" (
                start "Celeborn Master" cmd /c call "%CELEBORN_HOME%\sbin\start-master.cmd"
            ) else (
                echo Starting master on %%i
                if exist "%WINDIR%\System32\PSExec.exe" (
                    start "Celeborn Master %%i" PSExec \\%%i -d "%CELEBORN_HOME%\sbin\start-master.cmd"
                ) else (
                    echo Warning: PSExec not found. Install PSTools to start services on remote machines.
                    echo Starting master only on localhost...
                    start "Celeborn Master" cmd /c call "%CELEBORN_HOME%\sbin\start-master.cmd"
                )
            )
        )
    )
)
:skip_master

:: Wait for master to be ready
timeout /t 5 /nobreak > nul

:: Start workers
for %%h in ("%HOST_LIST:^=";"%") do (
    set "LINE=%%~h"
    if "!LINE!"=="" goto :skip_worker
    echo !LINE! | findstr /i "\[worker\]" >nul
    if not errorlevel 1 (
        for /f "tokens=2" %%i in ("!LINE!") do (
            if "%%i"=="localhost" (
                start "Celeborn Worker" cmd /c call "%CELEBORN_HOME%\sbin\start-worker.cmd"
            ) else (
                echo Starting worker on %%i
                if exist "%WINDIR%\System32\PSExec.exe" (
                    start "Celeborn Worker %%i" PSExec \\%%i -d "%CELEBORN_HOME%\sbin\start-worker.cmd"
                ) else (
                    echo Warning: PSExec not found. Install PSTools to start services on remote machines.
                    echo Starting worker only on localhost...
                    start "Celeborn Worker" cmd /c call "%CELEBORN_HOME%\sbin\start-worker.cmd"
                )
            )
            if defined CELEBORN_SLEEP (
                timeout /t %CELEBORN_SLEEP% /nobreak > nul
            )
        )
    )
)
:skip_worker

echo All Celeborn services have been started.
:: Don't use 'exit /b' here as we want the window to stay open if run directly
if "%1"=="" pause