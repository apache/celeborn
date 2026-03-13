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

ARG builder_java_image_tag=17-jdk-noble
ARG java_image_tag=17-jdk-noble

FROM eclipse-temurin:${builder_java_image_tag} AS dist-builder

ARG TARGETARCH
ARG DIST_PROFILES="-Pspark-3.5 -Pjdk-17 -Paws"

WORKDIR /src

RUN set -ex && \
    apt-get update && \
    apt-get install -y bash curl git gzip maven tar && \
    rm -rf /var/cache/apt/*

COPY . /src

RUN set -ex && \
    dist_profiles="${DIST_PROFILES}" && \
    if [ "${TARGETARCH}" = "arm64" ]; then \
      dist_profiles="${dist_profiles} -Paarch64"; \
    fi && \
    ./build/make-distribution.sh --mvn mvn ${dist_profiles}

FROM eclipse-temurin:${java_image_tag} AS runtime-base

ARG celeborn_uid=10006
ARG celeborn_gid=10006

ENV CELEBORN_HOME=/opt/celeborn
ENV CELEBORN_PREFER_JEMALLOC=true
ENV PATH=${PATH}:/opt/busybox:${CELEBORN_HOME}/sbin:${CELEBORN_HOME}/bin

RUN set -ex && \
    apt-get update && \
    apt-get install -y bash tini busybox bind9-utils telnet net-tools procps krb5-user dnsutils libjemalloc-dev && \
    ln -snf /bin/bash /bin/sh && \
    rm -rf /var/cache/apt/* && \
    mkdir /opt/busybox && \
    busybox --install /opt/busybox && \
    groupadd --gid=${celeborn_gid} celeborn && \
    useradd  --uid=${celeborn_uid} --gid=${celeborn_gid} celeborn -d /home/celeborn -m && \
    mkdir -p ${CELEBORN_HOME}

FROM runtime-base AS runtime-from-source

COPY --from=dist-builder /src/dist/bin /opt/celeborn/bin
COPY --from=dist-builder /src/dist/sbin /opt/celeborn/sbin
COPY --from=dist-builder /src/dist/conf /opt/celeborn/conf
COPY --from=dist-builder /src/dist/jars /opt/celeborn/jars
COPY --from=dist-builder /src/dist/master-jars /opt/celeborn/master-jars
COPY --from=dist-builder /src/dist/worker-jars /opt/celeborn/worker-jars
COPY --from=dist-builder /src/dist/cli-jars /opt/celeborn/cli-jars
COPY --from=dist-builder /src/dist/RELEASE /opt/celeborn/RELEASE

RUN chown -R celeborn:celeborn ${CELEBORN_HOME} && \
    chmod -R ug+rw ${CELEBORN_HOME} && \
    chmod a+x ${CELEBORN_HOME}/bin/* && \
    chmod a+x ${CELEBORN_HOME}/sbin/*

USER celeborn
WORKDIR /opt/celeborn

ENTRYPOINT ["/usr/bin/tini", "--"]

FROM runtime-base AS runtime-from-dist

COPY bin /opt/celeborn/bin
COPY sbin /opt/celeborn/sbin
COPY conf /opt/celeborn/conf
COPY jars /opt/celeborn/jars
COPY master-jars /opt/celeborn/master-jars
COPY worker-jars /opt/celeborn/worker-jars
COPY cli-jars /opt/celeborn/cli-jars
COPY RELEASE /opt/celeborn/RELEASE

RUN chown -R celeborn:celeborn ${CELEBORN_HOME} && \
    chmod -R ug+rw ${CELEBORN_HOME} && \
    chmod a+x ${CELEBORN_HOME}/bin/* && \
    chmod a+x ${CELEBORN_HOME}/sbin/*

USER celeborn
WORKDIR /opt/celeborn

ENTRYPOINT ["/usr/bin/tini", "--"]
