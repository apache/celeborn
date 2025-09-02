@echo off

SET HADOOP_USER_NAME=mtpkrbrs

if not defined JAVA_HOME (
  echo JAVA_HOME is not defined use D:\data\java.latest
  set JAVA_HOME=D:\data\java.latest
)
if not defined HADOOP_CONF_DIR (
  echo HADOOP_CONF_DIR is not defined use D:\data\hadoop.config\latest
  set HADOOP_CONF_DIR=D:\data\hadoop.config\latest
)

if not defined HADOOP_HOME (
  echo HADOOP_HOME is not defined use D:\data\hadoop.latest
  set HADOOP_HOME=D:\data\hadoop.latest
)

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