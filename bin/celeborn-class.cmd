@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

rem Enable delayed expansion for !variable! syntax
setlocal EnableDelayedExpansion

if not defined CELEBORN_HOME (
    set "CELEBORN_HOME=%~dp0.."
)

if not defined CELEBORN_CONF_DIR (
    set "CELEBORN_CONF_DIR=%CELEBORN_HOME%\conf"
)

rem print launch command for debug
if not defined CELEBORN_PRINT_LAUNCH_COMMAND (
    set CELEBORN_PRINT_LAUNCH_COMMAND=0
)

if not defined CELEBORN_ENV_LOADED (
    set CELEBORN_ENV_LOADED=1
    if exist "%CELEBORN_CONF_DIR%\celeborn-env.cmd" (
        call "%CELEBORN_CONF_DIR%\celeborn-env.cmd"
    )
)

rem Find CELEBORN jars and set LAUNCH_CLASS
set LAUNCH_CLASS=
:parse_args
if "%~1"=="" goto :check_class
if "%~1"=="org.apache.celeborn.service.deploy.master.Master" (
    set "LAUNCH_CLASS=org.apache.celeborn.service.deploy.master.Master"
) else if "%~1"=="org.apache.celeborn.service.deploy.worker.Worker" (
    set "LAUNCH_CLASS=org.apache.celeborn.service.deploy.worker.Worker"
)
shift
goto :parse_args

:check_class
if "%LAUNCH_CLASS%"=="org.apache.celeborn.service.deploy.master.Master" (
    if exist "%CELEBORN_HOME%\master-jars" (
        set "CELEBORN_JARS_DIR=%CELEBORN_HOME%\master-jars"
    ) else (
        set "CELEBORN_JARS_DIR=%CELEBORN_HOME%\master\target"
    )
)

if "%LAUNCH_CLASS%"=="org.apache.celeborn.service.deploy.worker.Worker" (
    if exist "%CELEBORN_HOME%\worker-jars" (
        set "CELEBORN_JARS_DIR=%CELEBORN_HOME%\worker-jars"
    ) else (
        set "CELEBORN_JARS_DIR=%CELEBORN_HOME%\worker\target"
    )
)

if not exist "!CELEBORN_JARS_DIR!" (
    echo Failed to find CELEBORN jars directory ^(!CELEBORN_JARS_DIR!^). 1>&2
    echo You need to build CELEBORN with the target "package" before running this program. 1>&2
    exit /b 1
)

rem Construct classpath
set HADOOP_YARN_HOME=%HADOOP_HOME%
set YARN_CONF_DIR=%HADOOP_CONF_DIR%
set HADOOP_BIN_PATH=%HADOOP_HOME%\bin

set DIR_ARGS=%HADOOP_HOME%\share\hadoop\common\hadoop-common-*.jar
set DIR_ARGS=%DIR_ARGS% %HADOOP_HOME%\share\hadoop\common\lib\*.jar
set DIR_ARGS=%DIR_ARGS% %HADOOP_HOME%\share\hadoop\hdfs\hadoop-hdfs-*.jar

echo|set /p="Class-Path:" > manifest.txt
for /f "delims=" %%i in ('dir %DIR_ARGS% /b/s') do (call :subroutine %%i)
echo[ >> manifest.txt
jar cmf manifest.txt classpath.jar

set "CELEBORN_CLASSPATH=%CELEBORN_CONF_DIR%;%HADOOP_CONF_DIR%;%CELEBORN_JARS_DIR%\*;%JAVA_TOOLS_JAR%;%cd%\classpath.jar;"

rem Construct Java command
set "CMD=%JAVA% -XX:+IgnoreUnrecognizedVMOptions %CELEBORN_JAVA_OPTS% -cp "%CELEBORN_CLASSPATH%" %LAUNCH_CLASS%"

rem Add all remaining arguments back
:build_args
if "%~1"=="" goto :execute
set "CMD=!CMD! %LAUNCH_CLASS%"
shift
goto :build_args

:execute
if "%CELEBORN_PRINT_LAUNCH_COMMAND%"=="1" (
    echo Start to launch !CMD!
)

call !CMD!
exit /b 1

:subroutine
set PT=%1
set PT=file:///%PT:\=/%

if x%PT:-sources.jar=%==x%PT% (
  if x%PT:-tests.jar=%==x%PT% (
    echo  %PT% >> manifest.txt
  )
)

endlocal


