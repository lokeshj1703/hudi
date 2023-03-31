#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Note:
#
# This script is to
#  - set the corresponding variables based on CI job's build profiles
#  - prepare Hudi bundle jars for mounting into Docker container for validation
#  - prepare test datasets for mounting into Docker container for validation
#
# This is to run by GitHub Actions CI tasks from the project root directory
# and it contains the CI environment-specific variables.

HUDI_VERSION=0.13.0-rc3
REPO_BASE_URL=https://repository.apache.org/content/repositories/orgapachehudi-1117/org/apache/hudi

echo "Validating $HUDI_VERSION bundles"

# choose versions based on build profiles
if [[ ${SPARK_PROFILE} == 'spark' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=2.3.9
  DERBY_VERSION=10.10.2.0
  FLINK_VERSION=1.13.6
  SPARK_VERSION=2.4.8
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1136hive239spark248
  hudi_flink_bundle_name=hudi-flink1.13-bundle
  hudi_hadoop_mr_bundle_name=hudi-hadoop-mr-bundle
  hudi_kafka_connect_bundle_name=hudi-kafka-connect-bundle
  hudi_spark_bundle_name=hudi-spark-bundle_2.11
  hudi_utilities_bundle_name=hudi-utilities-bundle_2.11
  hudi_utilities_slim_bundle_name=hudi-utilities-slim-bundle_2.11
  hudi_metaserver_server_bundle_name=hudi-metaserver-server-bundle
elif [[ ${SPARK_PROFILE} == 'spark2.4' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=2.3.9
  DERBY_VERSION=10.10.2.0
  FLINK_VERSION=1.13.6
  SPARK_VERSION=2.4.8
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1136hive239spark248
  hudi_flink_bundle_name=hudi-flink1.13-bundle
  hudi_hadoop_mr_bundle_name=hudi-hadoop-mr-bundle
  hudi_kafka_connect_bundle_name=hudi-kafka-connect-bundle
  hudi_spark_bundle_name=hudi-spark2.4-bundle_2.11
  hudi_utilities_bundle_name=hudi-utilities-bundle_2.11
  hudi_utilities_slim_bundle_name=hudi-utilities-slim-bundle_2.11
  hudi_metaserver_server_bundle_name=hudi-metaserver-server-bundle
elif [[ ${SPARK_PROFILE} == 'spark3.1' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.13.6
  SPARK_VERSION=3.1.3
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1136hive313spark313
  hudi_flink_bundle_name=hudi-flink1.13-bundle
  hudi_hadoop_mr_bundle_name=hudi-hadoop-mr-bundle
  hudi_kafka_connect_bundle_name=hudi-kafka-connect-bundle
  hudi_spark_bundle_name=hudi-spark3.1-bundle_2.12
  hudi_utilities_bundle_name=hudi-utilities-bundle_2.12
  hudi_utilities_slim_bundle_name=hudi-utilities-slim-bundle_2.12
  hudi_metaserver_server_bundle_name=hudi-metaserver-server-bundle
elif [[ ${SPARK_PROFILE} == 'spark3.2' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.14.6
  SPARK_VERSION=3.2.3
  SPARK_HADOOP_VERSION=2.7
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1146hive313spark323
  hudi_flink_bundle_name=hudi-flink1.14-bundle
  hudi_hadoop_mr_bundle_name=hudi-hadoop-mr-bundle
  hudi_kafka_connect_bundle_name=hudi-kafka-connect-bundle
  hudi_spark_bundle_name=hudi-spark3.2-bundle_2.12
  hudi_utilities_bundle_name=hudi-utilities-bundle_2.12
  hudi_utilities_slim_bundle_name=hudi-utilities-slim-bundle_2.12
  hudi_metaserver_server_bundle_name=hudi-metaserver-server-bundle
elif [[ ${SPARK_PROFILE} == 'spark3.3' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.15.3
  SPARK_VERSION=3.3.1
  SPARK_HADOOP_VERSION=2
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1153hive313spark331
  hudi_flink_bundle_name=hudi-flink1.15-bundle
  hudi_hadoop_mr_bundle_name=hudi-hadoop-mr-bundle
  hudi_kafka_connect_bundle_name=hudi-kafka-connect-bundle
  hudi_spark_bundle_name=hudi-spark3.3-bundle_2.12
  hudi_utilities_bundle_name=hudi-utilities-bundle_2.12
  hudi_utilities_slim_bundle_name=hudi-utilities-slim-bundle_2.12
  hudi_metaserver_server_bundle_name=hudi-metaserver-server-bundle
elif [[ ${SPARK_PROFILE} == 'spark3' ]]; then
  HADOOP_VERSION=2.7.7
  HIVE_VERSION=3.1.3
  DERBY_VERSION=10.14.1.0
  FLINK_VERSION=1.15.3
  SPARK_VERSION=3.3.1
  SPARK_HADOOP_VERSION=2
  CONFLUENT_VERSION=5.5.12
  KAFKA_CONNECT_HDFS_VERSION=10.1.13
  IMAGE_TAG=flink1153hive313spark331
  hudi_flink_bundle_name=hudi-flink1.15-bundle
  hudi_hadoop_mr_bundle_name=hudi-hadoop-mr-bundle
  hudi_kafka_connect_bundle_name=hudi-kafka-connect-bundle
  hudi_spark_bundle_name=hudi-spark3-bundle_2.12
  hudi_utilities_bundle_name=hudi-utilities-bundle_2.12
  hudi_utilities_slim_bundle_name=hudi-utilities-slim-bundle_2.12
  hudi_metaserver_server_bundle_name=hudi-metaserver-server-bundle
fi

# Copy bundle jars to temp dir for mounting
TMP_JARS_DIR=/tmp/jars/$(date +%s)
mkdir -p $TMP_JARS_DIR
wget -q $REPO_BASE_URL/$hudi_flink_bundle_name/$HUDI_VERSION/$hudi_flink_bundle_name-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
wget -q $REPO_BASE_URL/$hudi_hadoop_mr_bundle_name/$HUDI_VERSION/$hudi_hadoop_mr_bundle_name-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
wget -q $REPO_BASE_URL/$hudi_kafka_connect_bundle_name/$HUDI_VERSION/$hudi_kafka_connect_bundle_name-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
wget -q $REPO_BASE_URL/$hudi_spark_bundle_name/$HUDI_VERSION/$hudi_spark_bundle_name-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
wget -q $REPO_BASE_URL/$hudi_utilities_bundle_name/$HUDI_VERSION/$hudi_utilities_bundle_name-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
wget -q $REPO_BASE_URL/$hudi_utilities_slim_bundle_name/$HUDI_VERSION/$hudi_utilities_slim_bundle_name-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
wget -q $REPO_BASE_URL/$hudi_metaserver_server_bundle_name/$HUDI_VERSION/$hudi_metaserver_server_bundle_name-$HUDI_VERSION.jar -P $TMP_JARS_DIR/
echo "Downloaded these jars from $REPO_BASE_URL for validation:"
ls -l $TMP_JARS_DIR

# Copy test dataset
TMP_DATA_DIR=/tmp/data/$(date +%s)
mkdir -p $TMP_DATA_DIR/stocks/data
cp ${GITHUB_WORKSPACE}/docker/demo/data/*.json $TMP_DATA_DIR/stocks/data/
cp ${GITHUB_WORKSPACE}/docker/demo/config/schema.avsc $TMP_DATA_DIR/stocks/

# build docker image
cd ${GITHUB_WORKSPACE}/packaging/bundle-validation || exit 1
docker build \
--build-arg HADOOP_VERSION=$HADOOP_VERSION \
--build-arg HIVE_VERSION=$HIVE_VERSION \
--build-arg DERBY_VERSION=$DERBY_VERSION \
--build-arg FLINK_VERSION=$FLINK_VERSION \
--build-arg SPARK_VERSION=$SPARK_VERSION \
--build-arg SPARK_HADOOP_VERSION=$SPARK_HADOOP_VERSION \
--build-arg CONFLUENT_VERSION=$CONFLUENT_VERSION \
--build-arg KAFKA_CONNECT_HDFS_VERSION=$KAFKA_CONNECT_HDFS_VERSION \
--build-arg IMAGE_TAG=$IMAGE_TAG \
-t hudi-ci-bundle-validation:$IMAGE_TAG \
.

# run validation script in docker
docker run -v $TMP_JARS_DIR:/opt/bundle-validation/jars -v $TMP_DATA_DIR:/opt/bundle-validation/data \
  -i hudi-ci-bundle-validation:$IMAGE_TAG bash validate.sh
