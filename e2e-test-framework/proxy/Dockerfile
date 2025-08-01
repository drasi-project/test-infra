# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM rust:1.82 AS builder
RUN apt-get update && apt-get install -y protobuf-compiler libcurl4 && apt-get clean

WORKDIR /usr/src
COPY ./proxy ./proxy

WORKDIR /usr/src/proxy
RUN cargo install --force --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y \
    gdb curl wget vim net-tools iputils-ping \
    && apt-get clean
ENV RUST_BACKTRACE=1
COPY --from=builder /usr/local/cargo/bin/proxy /usr/local/bin/proxy
CMD ["proxy"]