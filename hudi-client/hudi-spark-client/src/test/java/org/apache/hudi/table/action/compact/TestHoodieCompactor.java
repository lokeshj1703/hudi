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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.SparkHoodieBloomIndexHelper;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.strategy.PartitionRegexBasedCompactionStrategy;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import com.codahale.metrics.Counter;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCompactor extends HoodieSparkClientTestHarness {

  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    // Initialize a local spark env
    initSparkContexts();

    // Create a temp folder as the base path
    initPath();
    storage = HoodieStorageUtils.getStorage(basePath, storageConf);
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  public HoodieWriteConfig getConfig() {
    return getConfig(1);
  }

  public HoodieWriteConfig getConfig(int numCommitsBeforeCompaction) {
    return getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(numCommitsBeforeCompaction).build())
        .withMetricsConfig(getMetricsConfig())
        .build();
  }

  private static HoodieMetricsConfig getMetricsConfig() {
    return HoodieMetricsConfig.newBuilder().on(true).withReporterType("INMEMORY").build();
  }

  private long getCompactionMetricCount(String metric) {
    HoodieMetrics metrics = writeClient.getMetrics();
    String metricName = metrics.getMetricsName("counter", metric);
    SortedMap<String, Counter> counters = metrics.getMetrics().getRegistry().getCounters();

    return counters.containsKey(metricName) ? counters.get(metricName).getCount() : 0;
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .withMemoryConfig(HoodieMemoryConfig.newBuilder().withMaxDFSStreamBufferSize(1 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
  }

  @Test
  public void testCompactionOnCopyOnWriteFail() throws Exception {
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(getConfig());) {
      HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
      String compactionInstantTime = writeClient.createNewInstantTime();
      assertThrows(HoodieNotSupportedException.class, () -> {
        table.scheduleCompaction(context, compactionInstantTime, Option.empty());
        table.compact(context, compactionInstantTime);
      });

      // Verify compaction.requested, compaction.completed metrics counts.
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
    }
  }

  @Test
  public void testCompactionEmpty() {
    HoodieWriteConfig config = getConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {

      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      String compactionInstantTime = writeClient.createNewInstantTime();
      Option<HoodieCompactionPlan> plan = table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      assertFalse(plan.isPresent(), "If there is nothing to compact, result will be empty");

      // Verify compaction.requested, compaction.completed metrics counts.
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
    }
  }

  @Test
  public void testScheduleCompactionWithInflightInstant() {
    HoodieWriteConfig config = getConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      // insert 100 records.
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // create one inflight instance.
      newCommitTime = "102";
      writeClient.startCommitWithTime(newCommitTime);
      metaClient.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // create one compaction instance before exist inflight instance.
      String compactionTime = "101";
      writeClient.scheduleCompactionAtInstant(compactionTime, Option.empty());
    }
  }

  @Test
  public void testNeedCompactionCondition() throws Exception {
    HoodieWriteConfig config = getConfig(3);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      // insert 100 records.
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      // commit 1
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // commit 2
      updateRecords(config, "101", records);

      // commit 3 (inflight)
      newCommitTime = "102";
      writeClient.startCommitWithTime(newCommitTime);
      metaClient.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // check that compaction will not be scheduled
      String compactionTime = "107";
      assertFalse(writeClient.scheduleCompactionAtInstant(compactionTime, Option.empty()));
    }
  }

  @Test
  public void testWriteStatusContentsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withMetricsConfig(getMetricsConfig())
        .build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 1000);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 1000 records across 5 commits to generate sufficient log files.
      int i = 1;
      for (; i < 5; i++) {
        newCommitTime = String.format("10%s", i);
        updateRecords(config, newCommitTime, records);
        assertLogFilesNumEqualsTo(config, i);
      }
      HoodieWriteMetadata result = compact(writeClient, String.format("10%s", i));
      verifyCompaction(result);

      // Verify compaction.requested, compaction.completed metrics counts.
      assertEquals(1, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
      assertEquals(1, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
    }
  }

  @Test
  public void testSpillingWhenCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withMemoryConfig(HoodieMemoryConfig.newBuilder()
            .withMaxMemoryMaxSize(1L, 1L).build()) // force spill
        .withMetricsConfig(getMetricsConfig())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // trigger 2 updates following with compaction
      for (int i = 1; i < 5; i += 2) {
        // Update all the 100 records
        newCommitTime = "10" + i;
        updateRecords(config, newCommitTime, records);

        assertLogFilesNumEqualsTo(config, 1);

        HoodieWriteMetadata result = compact(writeClient, "10" + (i + 1));
        verifyCompaction(result);

        // Verify compaction.requested, compaction.completed metrics counts.
        assertEquals(i / 2 + 1, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
        assertEquals(i / 2 + 1, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
      }
    }
  }

  private static Stream<Arguments> regexTestParameters() {
    Object[][] data = new Object[][] {
        {
          ".*", Arrays.asList("2015/03/16", "2015/03/17", "2016/03/15")
        },
        {
          "2017/.*/.*", Collections.emptyList()
        },
        {
          "2015/03/.*", Arrays.asList("2015/03/16", "2015/03/17")
        },
        {
          "2016/.*/.*", Arrays.asList("2016/03/15")
        }
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("regexTestParameters")
  public void testCompactionSpecifyPartition(String regex, List<String> expectedCompactedPartition) throws Exception {
    HoodieCompactionConfig.Builder builder = HoodieCompactionConfig.newBuilder()
        .withCompactionStrategy(new PartitionRegexBasedCompactionStrategy()).withMaxNumDeltaCommitsBeforeCompaction(1);
    builder.withCompactionSpecifyPartitionPathRegex(regex);
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(builder.build())
        .withMetricsConfig(getMetricsConfig()).build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // update 1 time
      newCommitTime = "101";
      updateRecords(config, newCommitTime, records);
      assertLogFilesNumEqualsTo(config, 1);

      // schedule compaction
      boolean scheduled = writeClient.scheduleCompactionAtInstant("102", Option.empty());
      if (expectedCompactedPartition.isEmpty()) {
        assertFalse(scheduled);
        return;
      }

      HoodieWriteMetadata result = compact(writeClient, "102");

      assertTrue(result.getWriteStats().isPresent());
      List<HoodieWriteStat> stats = (List<HoodieWriteStat>) result.getWriteStats().get();
      assertEquals(expectedCompactedPartition.size(), stats.size());
      expectedCompactedPartition.forEach(expectedPartition -> {
        assertTrue(stats.stream().anyMatch(stat -> stat.getPartitionPath().contentEquals(expectedPartition)));
      });
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private void updateRecords(HoodieWriteConfig config, String newCommitTime, List<HoodieRecord> records) throws IOException {
    HoodieTable table = HoodieSparkTable.create(config, context);
    List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
    JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
    HoodieIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = tagLocation(index, updatedRecordsRDD, table);

    writeClient.startCommitWithTime(newCommitTime);
    writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, newCommitTime).collect();
    metaClient.reloadActiveTimeline();
  }

  /**
   * Verify that all data file has {@code expected} number of log files.
   *
   * @param config   The writer config
   * @param expected The expected number of log files
   */
  private void assertLogFilesNumEqualsTo(HoodieWriteConfig config, int expected) {
    HoodieTable table = HoodieSparkTable.create(config, context);
    for (String partitionPath : dataGen.getPartitionPaths()) {
      List<FileSlice> groupedLogFiles =
          table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
      for (FileSlice fileSlice : groupedLogFiles) {
        assertEquals(expected, fileSlice.getLogFiles().count(), "There should be " + expected + " log file written for every data file");
      }
    }
  }

  /**
   * Do a compaction.
   */
  private HoodieWriteMetadata compact(SparkRDDWriteClient writeClient, String compactionInstantTime) {
    writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
    HoodieWriteMetadata compactMetadata = writeClient.compact(compactionInstantTime);
    return compactMetadata;
  }

  /**
   * Verify that all partition paths are present in the HoodieWriteMetadata result.
   */
  private void verifyCompaction(HoodieWriteMetadata compactionMetadata) {
    assertTrue(compactionMetadata.getWriteStats().isPresent());
    List<HoodieWriteStat> stats = (List<HoodieWriteStat>) compactionMetadata.getWriteStats().get();
    assertEquals(dataGen.getPartitionPaths().length, stats.size());
    for (String partitionPath : dataGen.getPartitionPaths()) {
      assertTrue(stats.stream().anyMatch(stat -> stat.getPartitionPath().contentEquals(partitionPath)));
    }
    stats.forEach(stat -> {
      HoodieWriteStat.RuntimeStats runtimeStats = stat.getRuntimeStats();
      assertNotNull(runtimeStats);
      assertEquals(0, runtimeStats.getTotalCreateTime());
      assertTrue(runtimeStats.getTotalUpsertTime() > 0);
      assertTrue(runtimeStats.getTotalScanTime() > 0);
    });
  }
}
