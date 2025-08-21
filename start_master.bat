@echo off

if exist d:\data\celeborn.latest\installed.flag (
    rem Hack. Give celeborn chance to delete flag during restart before copying new version. In other case we find old flag and data will be deleted in next couple seconds
    sleep 7
    if exist d:\data\celeborn.latest\installed.flag (
      echo Setting CELEBORN_HOME=d:\data\celeborn.latest
      set CELEBORN_HOME=d:\data\celeborn.latest
    ) else (
      echo Celeborn not yet installed
      exit /b 1
    )
) else (
    echo Celeborn not yet installed
    exit /b 1
)

cd "D:\data\celeborn.latest"
call sbin\\start-master.cmd