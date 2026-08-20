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

# Spark image pre-wired to use Celeborn as its shuffle service.
#
# Built by docker/build-image.sh, which copies the Celeborn client shaded jar
# produced by make-distribution.sh (under dist/spark/) into this directory so
# it can be COPYed into /opt/spark/jars. The jar is not committed to the repo.
#
# Usage (built by build-image.sh, not directly):
#   docker build -t celeborn-spark:dev docker/spark/

ARG spark_image_tag=3.5.9
FROM apache/spark:${spark_image_tag}

ENV PATH="${PATH}:/opt/spark/bin"

USER root
RUN mkdir -p /home/spark /shared && chown spark:spark /home/spark /shared

# Celeborn client shaded jar enables spark.shuffle.manager=...CelebornShuffleManager
COPY celeborn-client-spark-*-shaded_*.jar /opt/spark/jars/

VOLUME /shared

USER spark
