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

:: included in all the celeborn scripts
:: should not be executable directly
:: also should not be passed any arguments, since we need original %*

:: symlink and absolute path should rely on CELEBORN_HOME to resolve
if not defined CELEBORN_HOME (
    pushd "%~dp0.."
    set "CELEBORN_HOME=%CD%"
    popd
)

if not defined CELEBORN_CONF_DIR (
    set "CELEBORN_CONF_DIR=%CELEBORN_HOME%\conf"
)

if not defined CELEBORN_ENV_LOADED (
    set "CELEBORN_ENV_LOADED=1"
    if exist "%CELEBORN_CONF_DIR%\celeborn-env.cmd" (
        call "%CELEBORN_CONF_DIR%\celeborn-env.cmd"
    )
)

if not defined CELEBORN_LOG_DIR set CELEBORN_LOG_DIR=D:\data\Logs\local\celeborn

:: Find the java binary
if defined JAVA_HOME (
    set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
    where java.exe >nul 2>&1
    if !errorlevel! equ 0 (
        for /f "tokens=*" %%i in ('where java.exe') do set "JAVA=%%i"
        for /f "tokens=*" %%i in ('"%JAVA%" -XshowSettings:properties -version 2^>^&1 ^| findstr "java.home"') do (
            for /f "tokens=3" %%j in ("%%i") do set "JAVA_HOME=%%j"
        )
    ) else (
        echo JAVA_HOME is not set >&2
        exit /b 1
    )
)

:: Find the java tools.jar
if exist "%JAVA_HOME%\lib\tools.jar" (
    set "JAVA_TOOLS_JAR=%JAVA_HOME%\lib\tools.jar"
) else if exist "%JAVA_HOME%\..\lib\tools.jar" (
    set "JAVA_TOOLS_JAR=%JAVA_HOME%\..\lib\tools.jar"
) else (
    echo WARNING: cannot locate tools.jar. Expected to find it in either %JAVA_HOME%\lib\tools.jar or %JAVA_HOME%\..\lib\tools.jar
)

:: Get log directory
if defined LOG_DIRS (
    set "CELEBORN_LOG_DIR=%LOG_DIRS%"
)
if not defined CELEBORN_LOG_DIR (
    set "CELEBORN_LOG_DIR=%CELEBORN_HOME%\logs"
)
if not exist "%CELEBORN_LOG_DIR%" (
    mkdir "%CELEBORN_LOG_DIR%"
)

:: Set PID directory
if not defined CELEBORN_PID_DIR (
    set "CELEBORN_PID_DIR=%CELEBORN_HOME%\pids"
)

set

:: Expand environment variables
call %~dp0expand-copy.bat %~dp0..\conf\celeborn-defaults-before-expand.conf %~dp0..\conf\celeborn-defaults.conf

endlocal & (
    set "CELEBORN_HOME=%CELEBORN_HOME%"
    set "CELEBORN_CONF_DIR=%CELEBORN_CONF_DIR%"
    set "CELEBORN_LOG_DIR=%CELEBORN_LOG_DIR%"
    set "CELEBORN_PID_DIR=%CELEBORN_PID_DIR%"
    set "JAVA=%JAVA%"
    set "JAVA_HOME=%JAVA_HOME%"
    if defined JAVA_TOOLS_JAR set "JAVA_TOOLS_JAR=%JAVA_TOOLS_JAR%"
)