/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.keygen

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConversions._

class TestRecordKeyGenerator extends SparkClientFunctionalTestHarness {

  var commonOpts: Map[String, String] = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "4",
      "hoodie.delete.shuffle.parallelism" -> "2",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @ParameterizedTest
  @CsvSource(value = Array(
      "COPY_ON_WRITE|org.apache.hudi.keygen.SimpleKeyGenerator",
      "COPY_ON_WRITE|org.apache.hudi.keygen.ComplexKeyGenerator",
      "COPY_ON_WRITE|org.apache.hudi.keygen.TimestampBasedKeyGenerator",
      "MERGE_ON_READ|org.apache.hudi.keygen.SimpleKeyGenerator",
      "MERGE_ON_READ|org.apache.hudi.keygen.ComplexKeyGenerator",
      "MERGE_ON_READ|org.apache.hudi.keygen.TimestampBasedKeyGenerator"
  ), delimiter = '|')
  def testRecordKeyGeneration(tableType: String, keyGenClass: String): Unit = {
    var options: Map[String, String] = commonOpts +
      (DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> keyGenClass) +
      (DataSourceWriteOptions.TABLE_TYPE.key() -> tableType)
    if (keyGenClass == classOf[TimestampBasedKeyGenerator].getName) {
      options ++= Map(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP -> "DATE_STRING",
        KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP -> "yyyy-MM-dd",
        KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP -> "yyyy/MM/dd")
    }

    // order of cols in inputDf and hudiDf differs slightly. so had to choose columns specifically to compare df directly.
    val dataGen = new HoodieTestDataGenerator(0xDEED)
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Insert Operation
    val records0 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDf0 = spark.read.json(spark.sparkContext.parallelize(records0, 2))
    inputDf0.write.format("org.apache.hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Overwrite)
        .save(basePath)

    inputDf0.show()

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Snapshot query
    val snapshotDf1 = spark.read.format("org.apache.hudi")
        .load(basePath)
    snapshotDf1.cache()

    val records1 = recordsToStrings(dataGen.generateUniqueUpdates("001", 50)).toList
    val updateDf = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    updateDf.write.format("org.apache.hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)

    val snapshotDf2 = spark.read.format("hudi")
        .load(basePath)
    snapshotDf2.cache()
    assertEquals(100, snapshotDf2.count())
  }

}
