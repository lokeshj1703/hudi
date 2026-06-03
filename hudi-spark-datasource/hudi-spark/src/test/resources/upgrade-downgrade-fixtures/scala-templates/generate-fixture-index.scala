/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.sql.SaveMode
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.DataSourceWriteOptions._
import spark.implicits._

val baseDir = "${BASE_PATH}"

println("Generating MOR index tables for BLOOM, GLOBAL_BLOOM, SIMPLE, GLOBAL_SIMPLE, BUCKET, and RECORD_INDEX index types...")

// (shortName, indexType, extraIndexConfig)
// - GLOBAL_BLOOM / GLOBAL_SIMPLE enforce key uniqueness across all partitions
// - BUCKET requires num.buckets and disables clustering (incompatible with bucket index)
// - RECORD_INDEX is the 0.14.0 metadata-table-based RLI; requires metadata enabled
// Note: RECORD_LEVEL_INDEX / GLOBAL_RECORD_LEVEL_INDEX are 1.x only; not supported in 0.14.0
val indexTypes = Seq(
  ("bloom",         "BLOOM",        Map.empty[String, String]),
  ("global-bloom",  "GLOBAL_BLOOM", Map.empty[String, String]),
  ("simple",        "SIMPLE",       Map.empty[String, String]),
  ("global-simple", "GLOBAL_SIMPLE",Map.empty[String, String]),
  ("bucket",        "BUCKET",       Map("hoodie.bucket.index.num.buckets" -> "4")),
  ("record-index",  "RECORD_INDEX", Map("hoodie.metadata.enable" -> "true", "hoodie.metadata.record.index.enable" -> "true"))
)

val initialData = Seq(
  ("id1", "Alice",   1000L, "2023-01-01"),
  ("id2", "Bob",     1001L, "2023-01-01"),
  ("id3", "Charlie", 1002L, "2023-01-01"),
  ("id4", "David",   1003L, "2023-01-02"),
  ("id5", "Eve",     1004L, "2023-01-02")
)

def createIndexTable(indexName: String, indexType: String, extraIndexConfig: Map[String, String]): Unit = {
  val tableName    = s"hudi_v6_table_index_$indexName"
  val tableBasePath = s"$baseDir/hudi-v6-table-index-$indexName"

  println(s"Creating MOR table for index type: $indexType at $tableBasePath")

  val indexConfig = Map("hoodie.index.type" -> indexType) ++ extraIndexConfig

  // Bucket index is incompatible with clustering — disable it only for BUCKET.
  val clusteringEnabled = if (indexType == "BUCKET") "false" else "true"

  val morConfig = Map(
    "hoodie.compact.inline"                  -> "true",
    "hoodie.clustering.inline"               -> clusteringEnabled,
    "hoodie.compact.inline.max.delta.commits" -> "3",
    "hoodie.clustering.inline.max.commits"   -> "4",
    "hoodie.metadata.compact.max.delta.commits" -> "3",
    "hoodie.keep.min.commits"                -> "5",
    "hoodie.keep.max.commits"                -> "6",
    "hoodie.cleaner.commits.retained"        -> "5"
  )

  val initialSetupConfig = Map(
    HoodieTableConfig.PRECOMBINE_FIELD.key -> "ts",
    RECORDKEY_FIELD.key                    -> "id",
    PARTITIONPATH_FIELD.key                -> "partition",
    "hoodie.table.name"                    -> tableName,
    "hoodie.datasource.write.table.type"   -> "MERGE_ON_READ",
    "hoodie.parquet.max.file.size"         -> "2048",
    "hoodie.parquet.small.file.limit"      -> "1024",
    "hoodie.clustering.plan.strategy.small.file.limit"         -> "10240",
    "hoodie.clustering.plan.strategy.target.file.max.bytes"    -> "10240"
  )

  val archivalConfig = Map(
    "hoodie.archive.automatic"              -> "true",
    "hoodie.commits.archival.batch"         -> "1",
    "hoodie.archive.merge.files.batch.size" -> "1"
  )

  // Step 1: initial insert — establishes table type, schema, and index config
  initialData.toDF("id", "name", "ts", "partition").write.format("hudi")
    .options(initialSetupConfig ++ morConfig ++ indexConfig)
    .option("hoodie.datasource.write.operation", "insert")
    .mode(SaveMode.Overwrite)
    .save(tableBasePath)
  println(s"  [$indexType] Step 1: Initial data written")

  // Step 2: insert more small files (sets up clustering candidates)
  Seq(("id6", "Frank", 2000L, "2023-01-01"), ("id7", "Grace", 2001L, "2023-01-01"))
    .toDF("id", "name", "ts", "partition").write.format("hudi")
    .options(initialSetupConfig ++ morConfig ++ indexConfig)
    .option("hoodie.datasource.write.operation", "insert")
    .mode(SaveMode.Append)
    .save(tableBasePath)
  println(s"  [$indexType] Step 2: Added more small files")

  // Step 3: first upsert (delta commit 1)
  Seq(("id1", "Alice_v2", 3000L, "2023-01-01")).toDF("id", "name", "ts", "partition")
    .write.format("hudi").options(morConfig ++ indexConfig)
    .option("hoodie.datasource.write.operation", "upsert")
    .mode(SaveMode.Append).save(tableBasePath)
  println(s"  [$indexType] Step 3: First upsert")

  // Step 4: second upsert — triggers inline COMPACTION after 3 delta commits
  Seq(("id2", "Bob_v2", 4000L, "2023-01-01")).toDF("id", "name", "ts", "partition")
    .write.format("hudi").options(morConfig ++ indexConfig)
    .option("hoodie.datasource.write.operation", "upsert")
    .mode(SaveMode.Append).save(tableBasePath)
  println(s"  [$indexType] Step 4: Second upsert (triggers COMPACTION)")

  // Step 5: insert — triggers inline CLUSTERING (after 4 commits) and CLEANING
  Seq(("id8", "Final", 5000L, "2023-01-01")).toDF("id", "name", "ts", "partition")
    .write.format("hudi").options(morConfig ++ archivalConfig ++ indexConfig)
    .option("hoodie.datasource.write.operation", "insert")
    .mode(SaveMode.Append).save(tableBasePath)
  println(s"  [$indexType] Step 5: Insert (triggers CLUSTERING and CLEANING)")

  // Step 6: extra insert — triggers ARCHIVAL (keep.max.commits=6 exceeded)
  Seq(("id9", "Extra", 6000L, "2023-01-01")).toDF("id", "name", "ts", "partition")
    .write.format("hudi").options(morConfig ++ archivalConfig ++ indexConfig)
    .option("hoodie.datasource.write.operation", "insert")
    .mode(SaveMode.Append).save(tableBasePath)
  println(s"  [$indexType] Step 6: Extra insert (triggers ARCHIVAL)")

  // Step 7: another insert — ensures archival is completed
  Seq(("id10", "MoreExtra", 7000L, "2023-01-01")).toDF("id", "name", "ts", "partition")
    .write.format("hudi").options(morConfig ++ archivalConfig ++ indexConfig)
    .option("hoodie.datasource.write.operation", "insert")
    .mode(SaveMode.Append).save(tableBasePath)
  println(s"  [$indexType] Step 7: More inserts (ensures ARCHIVAL completed)")

  // Step 8: delete — leaves uncompacted log files for realistic MOR state
  Seq(("id1", "Alice_v2", 9000L, "2023-01-01")).toDF("id", "name", "ts", "partition")
    .write.format("hudi")
    .options(indexConfig)
    .option("hoodie.datasource.write.operation", "delete")
    .option("hoodie.compact.inline",   "false")
    .option("hoodie.clustering.inline","false")
    .option("hoodie.clean.automatic",  "false")
    .option("hoodie.archive.automatic","false")
    .mode(SaveMode.Append).save(tableBasePath)
  println(s"  [$indexType] Step 8: Delete (leaves uncompacted log files)")

  println(s"Completed MOR table for index type: $indexType")
}

indexTypes.foreach { case (name, indexType, extraConfig) =>
  createIndexTable(name, indexType, extraConfig)
}

println("All MOR index tables generated successfully!")
System.exit(0)
