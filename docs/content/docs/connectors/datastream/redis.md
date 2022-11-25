---
title: Redis
weight: 5
type: docs
aliases:
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Redis Connector

This connector provides sinks that can request document actions to an
[Redis](https://redis.io/). To use this connector, add the following 
dependencies to your project:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Redis version</th>
      <th class="text-left">Maven Dependency</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>7.x</td>
        <td>{{< connector_artifact flink-connector-redis-streams 3.0.0 >}}</td>
    </tr>
  </tbody>
</table>

{{< py_download_link "redis" >}}

Note that the streaming connectors are currently not part of the binary
distribution. See [here]({{< ref "docs/dev/configuration/overview" >}}) for information
about how to package the program with the libraries for cluster execution.

## Installing Redis

Instructions for setting up a Redis cluster can be found
[here](https://redis.io/docs/getting-started/installation/).


