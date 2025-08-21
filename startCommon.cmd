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

if not defined HADOOP_HOME @(
  set HADOOP_HOME=D:\data\hadoop.latest
)
set CELEBORN_HOME=D:\data\celeborn.latest