@echo off
setlocal EnableDelayedExpansion

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

@echo on

set "PROJECT_DIR=%~dp0.."
set "DIST_DIR=%PROJECT_DIR%\dist"
set "NAME=bin"
set "RELEASE=false"
set "MVN=%PROJECT_DIR%\build\mvn"
set "SBT=%PROJECT_DIR%\build\sbt"
set "SBT_ENABLED=false"

set "MVN=mvn"

:parse_args
set "ARG=%~1"

if "%~1"=="" goto after_args
if "%~1"=="--name" (
    set "NAME=bin-%~2"
    shift
    goto next_arg
)
if "%~1"=="--mvn" (
    set "MVN=%~2"
    shift
    goto next_arg
)
if "%~1"=="--release" (
    set "RELEASE=true"
    goto next_arg
)
if "%~1"=="--sbt-enabled" (
    set "SBT_ENABLED=true"
    goto next_arg
)
if "%~1"=="--help" (
    call :exit_with_usage
    goto :eof
)
if "%ARG:~0,2%"=="--" (
    echo Error: %~1 is not supported
    call :exit_with_usage
    goto :eof
)
:: this line should use wildcard to match any argument starting with '-'
if "%ARG:~0,1%"=="-" goto after_args
echo Error: %~1 is not supported
call :exit_with_usage
goto :eof

:next_arg
shift
goto parse_args

:after_args

if not defined JAVA_HOME (
    where java >nul 2>&1
    if !ERRORLEVEL! EQU 0 (
        for /f "tokens=*" %%i in ('where java') do (
            set "JAVA_PATH=%%i"
        )
        for %%i in (!JAVA_PATH!) do set "JAVA_HOME=%%~dpi.."
    )
)

if not defined JAVA_HOME (
    echo Error: JAVA_HOME is not set, cannot proceed.
    exit /b 1
)

if not defined MAVEN_OPTS (
    set "MAVEN_OPTS=-Xmx2g -XX:ReservedCodeCacheSize=1g"
)

where mvn >nul 2>&1
if !ERRORLEVEL! NEQ 0 (
    echo Could not locate Maven command: '%MVN%'.
    exit /b 1
)

:: Get Git revision if available
set "GITREVSTRING="
where git >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    for /f "tokens=*" %%i in ('git rev-parse --short HEAD 2^>nul') do (
        set "GITREV=%%i"
        if defined GITREV set "GITREVSTRING= (git revision !GITREV!)"
    )
)

cd /d "%PROJECT_DIR%"

:: Make directories
if exist "%DIST_DIR%" rd /s /q "%DIST_DIR%"
mkdir "%DIST_DIR%"

set "MVN_DIST_OPT=-DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip"

call :build_service %*
if !ERRORLEVEL! NEQ 0 exit /b !ERRORLEVEL!

:: Handle release builds
if "%RELEASE%"=="true" (
    call :build_spark_client -Pspark-2.4
    call :build_spark_client -Pspark-3.4
    call :build_spark_client -Pspark-3.5
    call :build_flink_client -Pflink-1.14
    call :build_flink_client -Pflink-1.15
    call :build_flink_client -Pflink-1.17
    call :build_flink_client -Pflink-1.18
    call :build_flink_client -Pflink-1.19
    call :build_mr_client -Pmr
) else (
    echo build client with %*
    set "ENGINE_COUNT=0"
    set "ENGINES=spark flink mr"
    for %%e in (%ENGINES%) do (
        echo %%e
        echo.%*| findstr /C:"%%e" >nul && set /a "ENGINE_COUNT+=1"
    )
    if !ENGINE_COUNT! EQU 0 (
        echo Skip building client.
    ) else if !ENGINE_COUNT! GEQ 2 (
        echo Error: unsupported build options: %*
        echo        currently we do not support compiling different engine clients at the same time.
        exit /b 1
    ) else (
        echo.%*| findstr /C:"spark" >nul && (
            echo build spark clients
            call :build_spark_client %*
        )
        echo.%*| findstr /C:"flink" >nul && (
            echo build flink clients
            call :build_flink_client %*
        )
        echo.%*| findstr /C:"mr" >nul && (
            echo build mr clients
            call :build_mr_client %*
        )
    )
)

:: Copy configuration templates
mkdir "%DIST_DIR%\conf" 2>nul
for %%f in ("%PROJECT_DIR%\conf\*.template") do (
    copy "%%f" "%DIST_DIR%\conf\" >nul
)

:: Copy shell scripts
mkdir "%DIST_DIR%\bin" 2>nul
mkdir "%DIST_DIR%\sbin" 2>nul
xcopy /S /E /I /Y "%PROJECT_DIR%\bin\*" "%DIST_DIR%\bin\" >nul
xcopy /S /E /I /Y "%PROJECT_DIR%\sbin\*" "%DIST_DIR%\sbin\" >nul

:: Copy db scripts
mkdir "%DIST_DIR%\db-scripts\sql" 2>nul
xcopy /S /E /I /Y "%PROJECT_DIR%\service\src\main\resources\sql\*" "%DIST_DIR%\db-scripts\sql\" >nul

:: Copy container related resources
mkdir "%DIST_DIR%\docker" 2>nul
copy "%PROJECT_DIR%\docker\Dockerfile" "%DIST_DIR%\docker\" >nul

:: Copy Helm Charts
mkdir "%DIST_DIR%\charts" 2>nul
xcopy /S /E /I /Y "%PROJECT_DIR%\charts\*" "%DIST_DIR%\charts\" >nul

:: Copy license files
if exist "%PROJECT_DIR%\LICENSE-binary" (
    copy "%PROJECT_DIR%\LICENSE-binary" "%DIST_DIR%\LICENSE"
    xcopy /E /I /Y "%PROJECT_DIR%\licenses-binary" "%DIST_DIR%\licenses"
    copy "%PROJECT_DIR%\NOTICE-binary" "%DIST_DIR%\NOTICE"
)

:: Create zip file
set "DIRNAME=apache-celeborn-%VERSION%-%NAME%"
set "TARGETDIR=%PROJECT_DIR%\%DIRNAME%"
if exist "%TARGETDIR%" rd /s /q "%TARGETDIR%"
xcopy /E /I /Y "%DIST_DIR%" "%TARGETDIR%"
powershell -command "Compress-Archive -Path '%TARGETDIR%' -DestinationPath '%PROJECT_DIR%\%DIRNAME%.zip' -Force"
rd /s /q "%TARGETDIR%"

exit /b 0

:exit_with_usage
echo make-distribution.cmd - tool for making binary distributions of Celeborn
echo.
echo usage:
echo make-distribution.cmd [--name ^<custom_name^>] [--release] [--sbt-enabled] [--mvn ^<mvn-command^>] [maven build options]
echo.
exit /b 1

:build_service
:: Get version and scala version
for /f "tokens=* usebackq" %%a in (`mvn help:evaluate -Dexpression^=project.version "%*" 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "VERSION=%%a"
)
for /f "tokens=* usebackq" %%a in (`mvn help:evaluate -Dexpression^=scala.binary.version "%*" 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "SCALA_VERSION=%%a"
)

echo Celeborn version is %VERSION%
echo Making apache-celeborn-%VERSION%-%NAME%.zip
echo Celeborn %VERSION%%GITREVSTRING%> "%DIST_DIR%\RELEASE"
echo Build flags: %*>> "%DIST_DIR%\RELEASE"

call mvn clean package %MVN_DIST_OPT% %*
if !ERRORLEVEL! NEQ 0 exit /b !ERRORLEVEL!

mkdir "%DIST_DIR%\jars"
mkdir "%DIST_DIR%\master-jars"
mkdir "%DIST_DIR%\worker-jars"

:: Copy master jars
copy "%PROJECT_DIR%\master\target\celeborn-master_%SCALA_VERSION%-%VERSION%.jar" "%DIST_DIR%\master-jars\"
copy "%PROJECT_DIR%\master\target\scala-%SCALA_VERSION%\jars\*.jar" "%DIST_DIR%\jars\"
cd /d "%DIST_DIR%\master-jars"
for %%f in ("%PROJECT_DIR%\master\target\scala-%SCALA_VERSION%\jars\*") do (
    mklink "%%~nxf" "..\jars\%%~nxf"
)

:: Copy worker jars
copy "%PROJECT_DIR%\worker\target\celeborn-worker_%SCALA_VERSION%-%VERSION%.jar" "%DIST_DIR%\worker-jars\"
copy "%PROJECT_DIR%\worker\target\scala-%SCALA_VERSION%\jars\*.jar" "%DIST_DIR%\jars\"
cd /d "%DIST_DIR%\worker-jars"
for %%f in ("%PROJECT_DIR%\worker\target\scala-%SCALA_VERSION%\jars\*") do (
    mklink "%%~nxf" "..\jars\%%~nxf"
)
cd /d "%PROJECT_DIR%"
exit /b 0

:build_spark_client
:: Get version and scala version
for /f "tokens=* usebackq" %%a in (`"%MVN%" help:evaluate -Dexpression^=project.version %* 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "VERSION=%%a"
)
for /f "tokens=* usebackq" %%a in (`"%MVN%" help:evaluate -Dexpression^=scala.binary.version %* 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "SCALA_VERSION=%%a"
)
for /f "tokens=* usebackq" %%a in (`"%MVN%" help:evaluate -Dexpression^=spark.version %* 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "SPARK_VERSION=%%a"
)
for /f "tokens=1 delims=." %%a in ("%SPARK_VERSION%") do set "SPARK_MAJOR_VERSION=%%a"

call mvn clean install %MVN_DIST_OPT% -pl :celeborn-client-spark-%SPARK_MAJOR_VERSION%-shaded_%SCALA_VERSION% -am %*
if !ERRORLEVEL! NEQ 0 exit /b !ERRORLEVEL!

mkdir "%DIST_DIR%\spark"
copy "%PROJECT_DIR%\client-spark\spark-%SPARK_MAJOR_VERSION%-shaded\target\celeborn-client-spark-%SPARK_MAJOR_VERSION%-shaded_%SCALA_VERSION%-%VERSION%.jar" "%DIST_DIR%\spark\"
exit /b 0

:build_flink_client
:: Get version and scala version
for /f "tokens=* usebackq" %%a in (`"%MVN%" help:evaluate -Dexpression^=flink.version %* 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "FLINK_VERSION=%%a"
)
for /f "tokens=* usebackq" %%a in (`"%MVN%" help:evaluate -Dexpression^=scala.binary.version %* 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "SCALA_VERSION=%%a"
)
for /f "tokens=1,2 delims=." %%a in ("%FLINK_VERSION%") do set "FLINK_BINARY_VERSION=%%a.%%b"

call "%MVN%" clean package %MVN_DIST_OPT% -pl :celeborn-client-flink-%FLINK_BINARY_VERSION%-shaded_%SCALA_VERSION% -am %*
if !ERRORLEVEL! NEQ 0 exit /b !ERRORLEVEL!

mkdir "%DIST_DIR%\flink"
copy "%PROJECT_DIR%\client-flink\flink-%FLINK_BINARY_VERSION%-shaded\target\celeborn-client-flink-%FLINK_BINARY_VERSION%-shaded_%SCALA_VERSION%-%VERSION%.jar" "%DIST_DIR%\flink\"
exit /b 0

:build_mr_client
:: Get version
for /f "tokens=* usebackq" %%a in (`"%MVN%" help:evaluate -Dexpression^=project.version %* 2^>^&1 ^| findstr /v "INFO" ^| findstr /v "WARNING"`) do (
    set "VERSION=%%a"
)

call "%MVN%" clean package %MVN_DIST_OPT% -pl :celeborn-client-mr-shaded_%SCALA_VERSION% -am %*
if !ERRORLEVEL! NEQ 0 exit /b !ERRORLEVEL!

mkdir "%DIST_DIR%\mr"
copy "%PROJECT_DIR%\client-mr\mr-shaded\target\celeborn-client-mr-shaded_%SCALA_VERSION%-%VERSION%.jar" "%DIST_DIR%\mr\"
exit /b 0