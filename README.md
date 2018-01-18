<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core)
[![Travis Build Status](https://app.travis-ci.com/apache/calcite.svg?branch=master)](https://app.travis-ci.com/github/apache/calcite)
[![CI Status](https://github.com/apache/calcite/workflows/CI/badge.svg?branch=master)](https://github.com/apache/calcite/actions?query=branch%3Amaster)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/apache/calcite?svg=true&branch=master)](https://ci.appveyor.com/project/ApacheSoftwareFoundation/calcite)

# Abount KyCalcite

KyCalcite is a customized calcite for better kylin use. 

Naming convension of different branch is kylin-{CALCITE_VERSION}.x, e.g. kylin-1.13.0.x

Naming convension of different releases (the name could be used for creating git tag, or pom version name), is {CALCITE_VERSION}-kylin-r{RELEASE_NUMBER}, e.g. 1.13.0-kylin-r1

For new KyCalcite releases we need to deploy it to our own Nexus server (kynexus.chinaeast.cloudapp.chinacloudapi.cn:8081), if you don't have enough permission please contact hongbin.ma@kyligence.io

**Since sonar does not allow overriding formal releases, you might choose to use a snapshot version name, e.g. 1.13.0-kylin-r1-SNAPSHOT**

steps:

1. publish the new kycalcite to nexus server
2. change kap and kylin's calcite pom dependency version
3. create a tag for the commit on which the new kycalcite is built from. Don't forget to push tags to server


# Apache Calcite

Apache Calcite is a dynamic data management framework.

It contains many of the pieces that comprise a typical
database management system but omits the storage primitives.
It provides an industry standard SQL parser and validator,
a customisable optimizer with pluggable rules and cost functions,
logical and physical algebraic operators, various transformation
algorithms from SQL to algebra (and the opposite), and many
adapters for executing SQL queries over Cassandra, Druid,
Elasticsearch, MongoDB, Kafka, and others, with minimal
configuration.

For more details, see the [home page](http://calcite.apache.org).
