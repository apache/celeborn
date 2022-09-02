#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG java_image_tag=8-jdk-focal
FROM eclipse-temurin:${java_image_tag}

ARG rss_version=0.1.1

USER root

RUN set -ex && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt-get install -y bash tini busybox bind9-utils telnet net-tools procps krb5-user && \
    rm /bin/sh && \
    ln -snf /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* && \
    mkdir -p /opt/

ADD rss-${rss_version}-bin-release.tar.gz /opt/
