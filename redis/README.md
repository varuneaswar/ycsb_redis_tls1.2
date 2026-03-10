<!--
Copyright (c) 2014 - 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on Redis. 

### 1. Start Redis

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:redis-binding -am clean package

### 4. Provide Redis Connection Parameters
    
Set host, port, password, and cluster mode in the workload you plan to run. 

- `redis.host`
- `redis.port`
- `redis.password`
  * Don't set the password if redis auth is disabled.
- `redis.cluster`
  * Set the cluster parameter to `true` if redis cluster mode is enabled.
  * Default is `false`.
- `redis.ssl`
  * Set to `true` to enable TLS/SSL encryption (required for Redis 7.4+ clusters with TLS).
  * Default is `false`.
- `redis.ssl.keystore.path`
  * Path to a JKS keystore file for mutual TLS (client certificate). Optional.
- `redis.ssl.keystore.password`
  * Password for the keystore file. Optional.
- `redis.ssl.truststore.path`
  * Path to a JKS truststore file to verify the Redis server certificate. Optional.
  * When omitted, the default JVM truststore is used.
- `redis.ssl.truststore.password`
  * Password for the truststore file. Optional.

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load redis -s -P workloads/workloada -p "redis.host=127.0.0.1" -p "redis.port=6379" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load redis -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run redis -s -P workloads/workloada > outputRun.txt

