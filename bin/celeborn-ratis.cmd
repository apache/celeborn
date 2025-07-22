@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

setlocal EnableDelayedExpansion

goto :main

:printUsage
echo Usage: celeborn-ratis COMMAND [GENERIC_COMMAND_OPTIONS] [COMMAND_ARGS]
echo.
echo COMMAND is one of:
echo   sh     Command line tool for Celeborn ratis
echo.
echo GENERIC_COMMAND_OPTIONS supports:
echo   -D^<property=value^> Use a value for a given ratis-shell property
echo.
echo Commands print help when invoked without parameters.
goto :eof

:runJavaClass
set CLASS_ARGS=
:parse_java_opts
if "%~1"=="" goto :execute_java
set "ARG=%~1"
if "%ARG:~0,2%"=="-D" (
    set "CELEBORN_RATIS_SHELL_JAVA_OPTS=!CELEBORN_RATIS_SHELL_JAVA_OPTS! %ARG%"
) else if "%ARG:~0,2%"=="-X" (
    set "CELEBORN_RATIS_SHELL_JAVA_OPTS=!CELEBORN_RATIS_SHELL_JAVA_OPTS! %ARG%"
) else if "%ARG:~0,9%"=="-agentlib" (
    set "CELEBORN_RATIS_SHELL_JAVA_OPTS=!CELEBORN_RATIS_SHELL_JAVA_OPTS! %ARG%"
) else if "%ARG:~0,10%"=="-javaagent" (
    set "CELEBORN_RATIS_SHELL_JAVA_OPTS=!CELEBORN_RATIS_SHELL_JAVA_OPTS! %ARG%"
) else (
    set "CLASS_ARGS=!CLASS_ARGS! %ARG%"
)
shift
goto :parse_java_opts

:execute_java
"%JAVA%" -cp "%CLASSPATH%" -XX:+IgnoreUnrecognizedVMOptions %CELEBORN_RATIS_SHELL_JAVA_OPTS% %CLASS% %PARAMETER% %CLASS_ARGS%
goto :eof

:main
if "%1"=="" (
    call :printUsage
    exit /b 1
)

set COMMAND=%1
shift

rem Load Celeborn related env
if not defined CELEBORN_HOME (
    set "CELEBORN_HOME=%~dp0.."
)

call "%CELEBORN_HOME%\sbin\load-celeborn-env.cmd"

rem Build classpath from master jars
set "CELEBORN_RATIS_SHELL_CLASSPATH="
for /r "%CELEBORN_HOME%\master-jars" %%i in (*.jar) do (
    if "!CELEBORN_RATIS_SHELL_CLASSPATH!"=="" (
        set "CELEBORN_RATIS_SHELL_CLASSPATH=%%i"
    ) else (
        set "CELEBORN_RATIS_SHELL_CLASSPATH=!CELEBORN_RATIS_SHELL_CLASSPATH!;%%i"
    )
)

set "CELEBORN_RATIS_SHELL_CLIENT_CLASSPATH=%CELEBORN_CONF_DIR%;%CELEBORN_RATIS_SHELL_CLASSPATH%"

if not exist "%CELEBORN_CONF_DIR%\ratis-log4j.properties" (
    echo %CELEBORN_CONF_DIR%\ratis-log4j.properties not exists! 1>&2
)

set "CELEBORN_RATIS_SHELL_JAVA_OPTS=%CELEBORN_RATIS_SHELL_JAVA_OPTS% -Dratis.shell.logs.dir=%CELEBORN_LOG_DIR%"
set "CELEBORN_RATIS_SHELL_JAVA_OPTS=%CELEBORN_RATIS_SHELL_JAVA_OPTS% -Dlog4j.configuration=file:%CELEBORN_CONF_DIR%\ratis-log4j.properties"
set "CELEBORN_RATIS_SHELL_JAVA_OPTS=%CELEBORN_RATIS_SHELL_JAVA_OPTS% -Dorg.apache.jasper.compiler.disablejsr199=true"
set "CELEBORN_RATIS_SHELL_JAVA_OPTS=%CELEBORN_RATIS_SHELL_JAVA_OPTS% -Djava.net.preferIPv4Stack=true"
set "CELEBORN_RATIS_SHELL_JAVA_OPTS=%CELEBORN_RATIS_SHELL_JAVA_OPTS% -Dorg.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads=false"

set "PARAMETER="

if "%COMMAND%"=="sh" (
    set "CLASS=org.apache.ratis.shell.cli.sh.RatisShell"
    set "CLASSPATH=%CELEBORN_RATIS_SHELL_CLIENT_CLASSPATH%"
    call :runJavaClass %*
) else (
    echo Unsupported command %COMMAND% 1>&2
    call :printUsage
    exit /b 1
)

endlocal