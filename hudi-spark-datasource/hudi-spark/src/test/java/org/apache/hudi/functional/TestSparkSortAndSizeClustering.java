/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSparkSortAndSizeClustering extends HoodieSparkClientTestHarness {


  private HoodieWriteConfig config;
  private HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0);

  public void setup(int maxFileSize) throws IOException {
    setup(maxFileSize, Collections.emptyMap());
  }

  public void setup(int maxFileSize, Map<String, String> options) throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initFileSystem();
    Properties props = getPropertiesForKeyGen(true);
    props.putAll(options);
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE, props);
    config = getConfigBuilder().withProps(props)
        .withAutoCommit(false)
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.RECENT_DAYS)
            .build())
        .build();

    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testClusteringWithRDD() throws IOException {
    writeAndClustering(false);
  }

  @Test
  public void testClusteringWithRow() throws IOException {
    writeAndClustering(true);
  }

  /**
   * Asserts that the schema persisted under HoodieCommitMetadata.SCHEMA_KEY in a completed
   * replace (clustering) commit does NOT contain Hudi meta fields like _hoodie_commit_time.
   * The schema stored in commit metadata is meant to be the user/write schema.
   */
  @Test
  public void testReplaceCommitSchemaHasNoMetaFields() throws Exception {
    setup(102400);
    config.setValue("hoodie.datasource.write.row.writer.enable", "false");
    config.setValue("hoodie.metadata.enable", "false");
    config.setValue("hoodie.clustering.plan.strategy.daybased.lookback.partitions", "1");
    config.setValue("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(1024 * 1024));
    config.setValue("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(2 * 1024 * 1024));

    int numRecords = 1000;
    writeData(HoodieActiveTimeline.createNewInstantTime(), numRecords, true);

    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    writeClient.cluster(clusteringTime, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant replaceInstant = metaClient.getActiveTimeline()
        .getCompletedReplaceTimeline()
        .filter(i -> i.getTimestamp().equals(clusteringTime))
        .firstInstant()
        .orElseThrow(() -> new AssertionError("No completed replace commit found for " + clusteringTime));

    HoodieReplaceCommitMetadata replaceCommitMetadata = metaClient.getActiveTimeline()
        .deserializeInstantContent(replaceInstant, HoodieReplaceCommitMetadata.class);
    assertSchemaHasNoMetaFields(replaceCommitMetadata, "replace (clustering) commit");
  }

  /**
   * Even when {@code config.getSchema()} is pre-polluted with Hudi meta fields
   * (simulating upstream paths like compaction reader-schema setup that may set
   * a schema-with-meta-fields back onto the write config), both ingestion and
   * clustering commits must persist a clean schema (without meta fields) under
   * {@link HoodieCommitMetadata#SCHEMA_KEY}. This guards the fix in
   * {@code BaseCommitActionExecutor#getSchemaToStoreInCommit()}.
   */
  @Test
  public void testCommitSchemaCleanedEvenWhenConfigSchemaHasMetaFields() throws Exception {
    setup(102400);
    config.setValue("hoodie.datasource.write.row.writer.enable", "false");
    config.setValue("hoodie.metadata.enable", "false");
    config.setValue("hoodie.clustering.plan.strategy.daybased.lookback.partitions", "1");
    config.setValue("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(1024 * 1024));
    config.setValue("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(2 * 1024 * 1024));

    // Pre-pollute the write config schema with Hudi meta fields.
    Schema pollutedSchema = HoodieAvroUtils.addMetadataFields(
        new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA));
    assertNotNull(pollutedSchema.getField(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        "Sanity check: polluted schema must contain meta fields");
    config.setSchema(pollutedSchema.toString());

    int numRecords = 1000;
    String ingestionTime = HoodieActiveTimeline.createNewInstantTime();
    writeData(ingestionTime, numRecords, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant ingestionInstant = metaClient.getActiveTimeline()
        .getCommitsTimeline()
        .filterCompletedInstants()
        .filter(i -> i.getTimestamp().equals(ingestionTime))
        .firstInstant()
        .orElseThrow(() -> new AssertionError("No completed ingestion commit found for " + ingestionTime));
    HoodieCommitMetadata ingestionMetadata = metaClient.getActiveTimeline()
        .deserializeInstantContent(ingestionInstant, HoodieCommitMetadata.class);
    assertSchemaHasNoMetaFields(ingestionMetadata, "ingestion commit");

    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    writeClient.cluster(clusteringTime, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant replaceInstant = metaClient.getActiveTimeline()
        .getCompletedReplaceTimeline()
        .filter(i -> i.getTimestamp().equals(clusteringTime))
        .firstInstant()
        .orElseThrow(() -> new AssertionError("No completed replace commit found for " + clusteringTime));
    HoodieReplaceCommitMetadata replaceMetadata = metaClient.getActiveTimeline()
        .deserializeInstantContent(replaceInstant, HoodieReplaceCommitMetadata.class);
    assertSchemaHasNoMetaFields(replaceMetadata, "replace (clustering) commit");
  }

  private static void assertSchemaHasNoMetaFields(HoodieCommitMetadata commitMetadata, String label) {
    String schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
    assertNotNull(schemaStr, label + " must persist a schema under SCHEMA_KEY");
    assertFalse(schemaStr.isEmpty(), label + " schema must not be empty");
    Schema storedSchema = new Schema.Parser().parse(schemaStr);
    List<String> metaFieldsPresent = HoodieRecord.HOODIE_META_COLUMNS.stream()
        .filter(metaField -> storedSchema.getField(metaField) != null)
        .collect(Collectors.toList());
    assertTrue(metaFieldsPresent.isEmpty(),
        label + " schema should not contain Hudi meta fields, but found: " + metaFieldsPresent
            + ". Stored schema: " + schemaStr);
  }

  public void writeAndClustering(boolean isRow) throws IOException {
    setup(102400);
    config.setValue("hoodie.datasource.write.row.writer.enable", String.valueOf(isRow));
    config.setValue("hoodie.metadata.enable", "false");
    config.setValue("hoodie.clustering.plan.strategy.daybased.lookback.partitions", "1");
    config.setValue("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(1024 * 1024));
    config.setValue("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(2 * 1024 * 1024));

    int numRecords = 1000;
    writeData(HoodieActiveTimeline.createNewInstantTime(), numRecords, true);

    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    HoodieClusteringPlan plan = ClusteringUtils.getClusteringPlan(
        metaClient, HoodieTimeline.getReplaceCommitRequestedInstant(clusteringTime)).map(Pair::getRight).get();

    List<HoodieClusteringGroup> inputGroups = plan.getInputGroups();
    Assertions.assertEquals(1, inputGroups.size(), "Clustering plan will contain 1 input group");

    Integer outputFileGroups = plan.getInputGroups().get(0).getNumOutputFileGroups();
    Assertions.assertEquals(2, outputFileGroups, "Clustering plan will generate 2 output groups");

    HoodieWriteMetadata writeMetadata = writeClient.cluster(clusteringTime, true);
    List<HoodieWriteStat> writeStats = (List<HoodieWriteStat>)writeMetadata.getWriteStats().get();
    Assertions.assertEquals(2, writeStats.size(), "Clustering should write 2 files");

    List<Row> rows = readRecords();
    Assertions.assertEquals(numRecords, rows.size());
    validateDecimalTypeAfterClustering(writeStats);
  }

  // Validate that clustering produces decimals in legacy format
  private void validateDecimalTypeAfterClustering(List<HoodieWriteStat> writeStats) {
    writeStats.stream().map(writeStat -> new Path(metaClient.getBasePathV2(), writeStat.getPath())).forEach(writtenPath -> {
      MessageType schema = ParquetUtils.readMetadata(hadoopConf, writtenPath)
          .getFileMetaData().getSchema();
      int index = schema.getFieldIndex("height");
      Type decimalType = schema.getFields().get(index);
      assertEquals("DECIMAL", decimalType.getOriginalType().toString());
      assertEquals("FIXED_LEN_BYTE_ARRAY", decimalType.asPrimitiveType().getPrimitiveTypeName().toString());
    });
  }

  private List<WriteStatus> writeData(String commitTime, int totalRecords, boolean doCommit) {
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    writeClient.startCommitWithTime(commitTime);
    List<WriteStatus> writeStatues = writeClient.insert(writeRecords, commitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);

    if (doCommit) {
      assertTrue(writeClient.commitStats(commitTime, context.parallelize(writeStatues, 1), writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType()));
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatues;
  }

  private List<Row> readRecords() {
    Dataset<Row> roViewDF = sparkSession
        .read()
        .format("hudi")
        .load(basePath + "/*/*/*/*");
    roViewDF.createOrReplaceTempView("clutering_table");
    return sparkSession.sqlContext().sql("select * from clutering_table").collectAsList();
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .forTable("clustering-table")
        .withEmbeddedTimelineServerEnabled(true);
  }
}
