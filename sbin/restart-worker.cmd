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

:: Restart the celeborn worker on the machine this script is executed on.

if not defined CELEBORN_HOME (
    pushd "%~dp0.."
    set "CELEBORN_HOME=%CD%"
    popd
)

call "%CELEBORN_HOME%\sbin\load-celeborn-env.cmd"

:: Load CELEBORN_WORKER_MEMORY and CELEBORN_WORKER_OFFHEAP_MEMORY from celeborn-defaults-before-expand.conf
:: (generated from celeborn.ini [celeborn-site] section by GenerateCelebornConfig.ps1).
set "WORKER_ENV_CONF=%CELEBORN_HOME%\conf\celeborn-defaults-before-expand.conf"
if exist "%WORKER_ENV_CONF%" (
    for /f "usebackq tokens=1,* delims==" %%k in ("%WORKER_ENV_CONF%") do (
        if "%%k"=="CELEBORN_WORKER_MEMORY" set "CELEBORN_WORKER_MEMORY=%%l"
        if "%%k"=="CELEBORN_WORKER_OFFHEAP_MEMORY" set "CELEBORN_WORKER_OFFHEAP_MEMORY=%%l"
    )
)

if not defined CELEBORN_WORKER_MEMORY (
    set CELEBORN_WORKER_MEMORY=2g
)

if not defined CELEBORN_WORKER_OFFHEAP_MEMORY (
    set CELEBORN_WORKER_OFFHEAP_MEMORY=6g
)

echo [%date% %time%] CELEBORN_WORKER_MEMORY=%CELEBORN_WORKER_MEMORY% CELEBORN_WORKER_OFFHEAP_MEMORY=%CELEBORN_WORKER_OFFHEAP_MEMORY%

set "CELEBORN_JAVA_OPTS=%CELEBORN_WORKER_JAVA_OPTS%"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% -Xmx%CELEBORN_WORKER_MEMORY%"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% -XX:MaxDirectMemorySize=%CELEBORN_WORKER_OFFHEAP_MEMORY%"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% -Dio.netty.tryReflectionSetAccessible=true"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --illegal-access=warn"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.lang=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.io=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.net=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.nio=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.util=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/sun.security.action=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
set "CELEBORN_JAVA_OPTS=%CELEBORN_JAVA_OPTS% -Dorg.apache.logging.log4j.level=INFO"

if not defined WORKER_INSTANCE (
    set WORKER_INSTANCE=1
)

call "%CELEBORN_HOME%\sbin\celeborn-daemon.cmd" restart org.apache.celeborn.service.deploy.worker.Worker %WORKER_INSTANCE% %*