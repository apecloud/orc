# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ORC compile for Ubuntu 18
#

FROM ubuntu:18.04
LABEL maintainer="Apache ORC project <dev@orc.apache.org>"
ARG jdk=8

RUN ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN apt-get update
RUN apt-get install -y \
  cmake \
  gcc \
  g++ \
  git \
  libsasl2-dev \
  libssl-dev \
  make \
  curl \
  maven \
  openjdk-${jdk}-jdk \
  tzdata
RUN update-alternatives --set java $(update-alternatives --list java | grep ${jdk}) && \
    update-alternatives --set javac $(update-alternatives --list javac | grep ${jdk})

WORKDIR /root
VOLUME /root/.m2/repository

CMD if [ ! -d orc ]; then \
      echo "No volume provided, building from apache main."; \
      echo "Pass '-v`pwd`:/root/orc' to docker run to build local source."; \
      git clone https://github.com/apache/orc.git -b main; \
    fi && \
    mkdir build && \
    cd build && \
    cmake ../orc && \
    make package test-out
