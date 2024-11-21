---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Celeborn DB ConfigStore Upgrade HowTo
============================

This document describes how to upgrade the schema of a MySQL backed
DB ConfigStore instance from one release version of Celeborn to another
release version of Celeborn. For example, by following the steps listed
below it is possible to upgrade Celeborn 0.5.0 DBConfigStore schema to
Celeborn 0.6.0 DB ConfigStore schema. Before attempting this project we
strongly recommend that you read through all the steps in this document
and familiarize yourself with the required tools.

ConfigStore Upgrade Steps
=======================

1) Shutdown your ConfigStore instance and restrict access to the
   ConfigStore's MySQL database. It is very important that no one else
   accesses or modifies the contents of database while you are
   performing the schema upgrade.

2) Create a backup of your MySQL ConfigStore database. This will allow
   you to revert any changes made during the upgrade process if
   something goes wrong. The mysqldump utility is the easiest way to
   create a backup of a MySQL database:

   % mysqldump --opt <configstore_db_name> > configstore_backup.sql

   Note that you may need also need to specify a hostname and username
   using the --host and --user command line switches.

3) Dump your configstore database schema to a file. We use the mysqldump
   utility again, but this time with a command line option that
   specifies we are only interested in dumping the DDL statements
   required to create the schema:

   % mysqldump --skip-add-drop-table --no-data <configstore_db_name> > celeborn-x.y.z-mysql.sql

4) The schema upgrade scripts assume that the schema you are upgrading
   closely matches the official schema for your particular version of
   Celeborn. The files in this directory with names like
   "celeborn-x.y.z-mysql.sql" contain dumps of the official schemas
   corresponding to each of the released versions of Celeborn. You can
   determine differences between your schema and the official schema
   by diffing the contents of the official dump with the schema dump
   you created in the previous step. Some differences are acceptable
   and will not interfere with the upgrade process, but others need to
   be resolved manually or the upgrade scripts will fail to complete.

5) You are now ready to run the schema upgrade scripts. If you are
   upgrading from Celeborn 0.5.0 to Celeborn 0.6.0 you need to run the
   upgrade-0.5.0-to-0.6.0-mysql.sql script, but if you are upgrading from
   0.6.0 to a future version like 0.7.0, you will need to run the 1.6.0 to
   0.7.0 upgrade script followed by the 0.6.0 to 0.7.0 upgrade script.

   % mysql --verbose
   mysql> use <configstore_db_name>;
   Database changed
   mysql> source upgrade-0.5.0-to-0.6.0-mysql.sql
   mysql> source upgrade-0.6.0-to-0.7.0-mysql.sql

   These scripts should run to completion without any errors. If you
   do encounter errors you need to analyze the cause and attempt to
   trace it back to one of the preceding steps.

6) The final step of the upgrade process is validating your freshly
   upgraded schema against the official schema for your particular
   version of Celeborn. This is accomplished by repeating steps (3) and
   (4), but this time comparing against the official version of the
   upgraded schema, e.g. if you upgraded the schema to Celeborn 0.6.0 then
   you will want to compare your schema dump against the contents of
   celeborn-0.6.0-mysql.sql
