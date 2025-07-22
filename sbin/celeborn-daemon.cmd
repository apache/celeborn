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

:: Runs a celeborn command as a daemon.
::
:: Environment Variables
::
::   CELEBORN_CONF_DIR  Alternate conf dir. Default is %CELEBORN_HOME%\conf.
::   CELEBORN_LOG_DIR   Where log files are stored. %CELEBORN_HOME%\logs by default.
::   CELEBORN_PID_DIR   The pid files are stored. %TEMP% by default.
::   CELEBORN_IDENT_STRING   A string representing this instance of celeborn. %USERNAME% by default
::   CELEBORN_NICENESS The scheduling priority for daemons. Defaults to 0.

goto :main

:usage
    echo Usage: celeborn-daemon.cmd [--config ^<conf-dir^>] (start^|stop^|restart^|status) ^<celeborn-class-name^> ^<celeborn-instance-number^> [args]
    echo.
    echo Options:
    echo   ^<conf-dir^>                  Override CELEBORN_CONF_DIR
    echo   ^<celeborn-class-name^>       The main class name of the celeborn command to run
    echo                              Example: org.apache.celeborn.service.deploy.master.Master for celeborn master
    echo                                       org.apache.celeborn.service.deploy.worker.Worker for celeborn worker
    echo   ^<celeborn-instance-number^>  The instance number of the celeborn command to run
    exit /b 1

:rotate_log
    set "log=%~1"
    set "num=5"
    if not "%~2"=="" set "num=%~2"
    if exist "%log%" (
        for /l %%i in (%num%,-1,2) do (
            set /a "prev=%%i-1"
            if exist "!log!.!prev!" ren "!log!.!prev!" "!log!.%%i"
        )
        if exist "%log%" ren "%log%" "%log%.1"
    )
    exit /b 0

:check_java_process
    set "pid_file=%~1"
    set "java_running="
    if exist "%pid_file%" (
        set /p pid=<"%pid_file%"
        for /f "tokens=*" %%a in ('tasklist /fi "PID eq !pid!" /fo csv ') do (
            echo %%a | findstr /i "java.exe" >nul && set "java_running=1"
        )
    )
    exit /b

:execute_command
    if not defined CELEBORN_NO_DAEMONIZE (
        cmd /c "%*" 
        for /f "tokens=2" %%a in ('tasklist /fi "imagename eq java.exe" /fo list ^| findstr "PID:"') do set newpid=%%a
        echo !newpid! > "%pid%"
        timeout /t 2 /nobreak > nul
        call :check_java_process "%pid%"
        if not defined java_running (
            echo Failed to launch: %*
            type "%log%"
            echo Full log in %log%
        )
    ) else (
        %*
    )
    exit /b

:run_command
    if not exist "%CELEBORN_PID_DIR%" mkdir "%CELEBORN_PID_DIR%"
    
    call :check_java_process "%pid%"
    if defined java_running (
        echo %command% running as process !pid!. Stop it first.
        exit /b 1
    )

    call :rotate_log "%log%"
    echo Starting %command%, logging to %log%

    if "%~1"=="class" (
        call :execute_command "%CELEBORN_HOME%\bin\celeborn-class.cmd" "%command%" %2 %3 %4 %5 %6 %7 %8 %9
    ) else (
        echo Unknown mode: %~1
        exit /b 1
    )
    exit /b

:start_celeborn
    call :run_command class %*
    exit /b

:stop_celeborn
    call :check_java_process "%pid%"
    if defined java_running (
        echo Stopping %command%
        taskkill /PID !pid! /F
        if errorlevel 1 (
            echo Failed to stop server(pid=!pid!)
            exit /b 1
        )
        del "%pid%" 2>nul
    ) else (
        echo No %command% to stop
    )
    exit /b

:check_celeborn
    call :check_java_process "%pid%"
    if defined java_running (
        echo %command% is running.
        exit /b 0
    ) else if exist "%pid%" (
        echo %pid% file is present but %command% not running
        exit /b 1
    ) else (
        echo %command% not running.
        exit /b 2
    )

:main
    if "%~1"=="" goto :usage

    :: Check if --config is passed as an argument
    if "%~1"=="--config" (
        set "conf_dir=%~2"
        if not exist "%conf_dir%" (
            echo ERROR: %conf_dir% is not a directory
            goto :usage
        )
        set "CELEBORN_CONF_DIR=%conf_dir%"
        shift /2
        shift /2
    )

    set "option=%~1"
    set "command=%~2"
    set "instance=%~3"
    shift /3

    if not defined CELEBORN_HOME (
        pushd "%~dp0.."
        set "CELEBORN_HOME=%CD%"
        popd
    )

    call "%CELEBORN_HOME%\sbin\load-celeborn-env.cmd"

    if not defined CELEBORN_IDENT_STRING set "CELEBORN_IDENT_STRING=%USERNAME%"

    set "log=%CELEBORN_LOG_DIR%\celeborn-%CELEBORN_IDENT_STRING%-%command%-%instance%-%COMPUTERNAME%.out"
    set "pid=%CELEBORN_PID_DIR%\celeborn-%CELEBORN_IDENT_STRING%-%command%-%instance%.pid"

    if not defined CELEBORN_NICENESS set "CELEBORN_NICENESS=0"

    if "%option%"=="start" (
        call :start_celeborn %*
    ) else if "%option%"=="stop" (
        call :stop_celeborn
    ) else if "%option%"=="restart" (
        echo Restarting Celeborn
        call :stop_celeborn
        call :start_celeborn %*
    ) else if "%option%"=="status" (
        call :check_celeborn
    ) else (
        goto :usage
    )

endlocal