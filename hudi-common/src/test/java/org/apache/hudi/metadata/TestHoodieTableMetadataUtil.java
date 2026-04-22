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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.FilePreFetcher;
import org.apache.hudi.io.storage.HoodieAvroHFileReader;
import org.apache.hudi.io.storage.HoodieAvroHFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieSeekingFileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_FILE_PRE_FETCHER_IMPLEMENTATION;
import static org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_FILE_PRE_FETCHER_THRESHOLD_SIZE_MB;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieTableMetadataUtil extends HoodieCommonTestHarness {

  private static HoodieTestTable hoodieTestTable;
  private static final List<String> DATE_PARTITIONS = Arrays.asList("2019/01/01", "2020/01/02", "2021/03/01");

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
    initTestDataGenerator(DATE_PARTITIONS.toArray(new String[0]));
    hoodieTestTable = HoodieTestTable.of(metaClient);
  }

  @AfterEach
  public void tearDown() throws IOException {
    metaClient.getFs().delete(metaClient.getBasePathV2(), true);
    cleanupTestDataGenerator();
    cleanMetaClient();
  }

  @Test
  public void testReadRecordKeysFromBaseFilesWithEmptyPartitionBaseFilePairs() {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    List<Pair<String, FileSlice>> partitionFileSlicePairs = Collections.emptyList();
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.readRecordKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        false,
        1,
        "activeModule",
        metaClient,
        EngineType.SPARK,
        false
    );
    assertTrue(result.isEmpty());
  }

  @Test
  public void testReadRecordKeysFromBaseFilesWithValidRecords() throws Exception {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    String instant = "20230918120000000";
    hoodieTestTable = hoodieTestTable.addCommit(instant);
    Set<String> recordKeys = new HashSet<>();
    final List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
    // Generate 10 inserts for each partition and populate partitionBaseFilePairs and recordKeys.
    DATE_PARTITIONS.forEach(p -> {
      try {
        List<HoodieRecord> hoodieRecords = dataGen.generateInsertsForPartition(instant, 10, p);
        String fileId = UUID.randomUUID().toString();
        FileSlice fileSlice = new FileSlice(p, instant, fileId);
        writeParquetFile(instant, hoodieTestTable.getBaseFilePath(p, fileId), hoodieRecords, metaClient, engineContext);
        HoodieBaseFile baseFile = new HoodieBaseFile(hoodieTestTable.getBaseFilePath(p, fileId).toString(), fileId, instant, null);
        fileSlice.setBaseFile(baseFile);
        partitionFileSlicePairs.add(Pair.of(p, fileSlice));
        recordKeys.addAll(hoodieRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Call the method readRecordKeysFromBaseFiles with the created partitionBaseFilePairs.
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.readRecordKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        false,
        1,
        "activeModule",
        metaClient,
        EngineType.SPARK,
        false
    );
    // Validate the result.
    List<HoodieRecord> records = result.collectAsList();
    assertEquals(30, records.size());
    assertEquals(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), records.get(0).getPartitionPath());
    for (HoodieRecord record : records) {
      assertTrue(recordKeys.contains(record.getRecordKey()));
    }
  }

  @ParameterizedTest
  @MethodSource("fileReaderTestParameters")
  void testFileReaderBehavior(String thresholdSizeMb, String fetcherImplementation, boolean expectedContentPresent) throws Exception {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    String instant = "20230918120000000";
    hoodieTestTable = hoodieTestTable.addCommit(instant);

    // Generate 10 inserts for each partition and populate partitionBaseFilePairs and recordKeys.
    String partition = DATE_PARTITIONS.get(0);
    List<HoodieRecord> hoodieRecords = dataGen.generateInsertsForPartition(instant, 10, partition);
    String fileId = UUID.randomUUID().toString();
    FileSlice fileSlice = new FileSlice(partition, instant, fileId);
    writeHFile(instant, hoodieTestTable.getBaseFilePath(partition, fileId, HoodieFileFormat.HFILE.getFileExtension()), hoodieRecords, metaClient, engineContext);
    HoodieBaseFile baseFile = new HoodieBaseFile(hoodieTestTable.getBaseFilePath(partition, fileId, HoodieFileFormat.HFILE.getFileExtension()).toString(), fileId, instant, null);
    fileSlice.setBaseFile(baseFile);

    HoodieTimer timer = HoodieTimer.start();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();

    // Set threshold size if provided
    if (thresholdSizeMb != null) {
      metadataConfig.setValue(METADATA_FILE_PRE_FETCHER_THRESHOLD_SIZE_MB.key(), thresholdSizeMb);
    }

    // Set fetcher implementation if provided
    if (fetcherImplementation != null) {
      metadataConfig.setValue(METADATA_FILE_PRE_FETCHER_IMPLEMENTATION.key(), fetcherImplementation);
    }

    Pair<HoodieSeekingFileReader<?>, Long> result = HoodieTableMetadataUtil.getBaseFileReader(fileSlice, timer, metadataConfig.getProps(), metaClient.getHadoopConf());
    HoodieAvroHFileReader reader = (HoodieAvroHFileReader) result.getLeft();

    // Validate content presence based on expected behavior
    assertEquals(expectedContentPresent, reader.getContent().isPresent());

    if (null != fetcherImplementation && fetcherImplementation.equals(TestFilePreFetcher.class.getName())) {
      assertEquals(1, TestFilePreFetcher.numTimesFetcherCalled.get());
    }
  }

  static Stream<Arguments> fileReaderTestParameters() {
    return Stream.of(
        // Test case 1: File size higher than threshold - use native reader (No caching)
        Arguments.of(null, null, false),

        // Test case 2: File size lower than threshold - uses cached reader
        Arguments.of("120", null, true),

        // Test case 3: File size lower than threshold with invalid fetcher - use native reader (No caching)
        Arguments.of("120", "org.apache.hudi.InvalidFetcher.class", false),

        // Test case 4: Custom file pre fetcher should use cached reader
        Arguments.of("120", TestFilePreFetcher.class.getName(), true)
    );
  }

  public static class TestFilePreFetcher extends FilePreFetcher {

    private static AtomicInteger numTimesFetcherCalled = new AtomicInteger(0);

    public TestFilePreFetcher(TypedProperties properties, Configuration hadoopConf) {
      super(properties, hadoopConf);
    }

    @Override
    public byte[] fetchFileContents(Path filePath, long fileLen) throws IOException {
      numTimesFetcherCalled.getAndIncrement();
      return super.fetchFileContents(filePath, fileLen);
    }
  }

  private static void writeParquetFile(String instant,
                                       Path path,
                                       List<HoodieRecord> records,
                                       HoodieTableMetaClient metaClient,
                                       HoodieLocalEngineContext engineContext) throws IOException {
    HoodieFileWriter writer = HoodieFileWriterFactory.getFileWriter(
        instant,
        path,
        metaClient.getHadoopConf(),
        metaClient.getTableConfig(),
        HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS,
        engineContext.getTaskContextSupplier(),
        HoodieRecord.HoodieRecordType.AVRO);
    for (HoodieRecord record : records) {
      writer.writeWithMetadata(record.getKey(), record, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS);
    }
    writer.close();
  }

  private static void writeHFile(String instant,
                                 Path path,
                                 List<HoodieRecord> records,
                                 HoodieTableMetaClient metaClient,
                                 HoodieLocalEngineContext engineContext) throws Exception {
    HoodieAvroHFileWriter writer = createWriter(instant, path, metaClient, engineContext);
    records.sort(Comparator.comparing(r -> r.getKey().getRecordKey()));

    for (HoodieRecord record : records) {
      writer.writeWithMetadata(record.getKey(), record, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS);
    }
    writer.close();
  }

  private static HoodieAvroHFileWriter createWriter(String instant,
                                                    Path path,
                                                    HoodieTableMetaClient metaClient,
                                                    HoodieLocalEngineContext engineContext) throws Exception {
    HoodieStorageConfig config = HoodieStorageConfig.newBuilder().build();
    return (HoodieAvroHFileWriter) HoodieFileWriterFactory.getFileWriter(
        instant, path, metaClient.getHadoopConf(), config,
        HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS,
        engineContext.getTaskContextSupplier(),
        HoodieRecord.HoodieRecordType.AVRO);
  }

  /**
   * Tests getValidInstantTimestamps rollback handling:
   * - Without MDT compaction, all rollback metadata is read (rolled-back commits appear in valid timestamps).
   * - With MDT compaction, only post-compaction rollback metadata is read (pre-compaction rollbacks are skipped
   *   because those log blocks are already merged into base files).
   */
  @Test
  void testGetValidInstantTimestampsSkipsPreCompactionRollbacks() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);

    String commit1 = "20260101010101000";
    String commit2 = "20260201010101000";
    String commit3 = "20260301010101000";
    String commit4 = "20260501010101000";
    String commit5 = "20260601010101000";
    testTable.addCommit(commit1);
    testTable.addCommit(commit2);
    testTable.addCommit(commit3);
    testTable.addCommit(commit4);
    testTable.addCommit(commit5);

    // Rollbacks before MDT compaction time
    addCompletedRollback(testTable, "20260202010101000", commit2);
    addCompletedRollback(testTable, "20260302010101000", commit3);
    // Rollback after MDT compaction time
    addCompletedRollback(testTable, "20260502010101000", commit4);

    // Delete rolled-back commit instants from the timeline to simulate real rollback behavior.
    // In a real system, the commit instant file is removed when a rollback completes, so the
    // only way these timestamps appear in validInstantTimestamps is via rollback metadata reading.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    for (String rolledBack : Arrays.asList(commit2, commit3, commit4)) {
      HoodieInstant completedCommit = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, rolledBack);
      metaClient.getActiveTimeline().deleteInstantFileIfExists(completedCommit);
    }

    // Create MDT metaClient with NO compaction initially (only delta commits)
    HoodieTableMetaClient mdtMetaClient = createMdtMetaClient();
    HoodieTestTable mdtTestTable = HoodieTestTable.of(mdtMetaClient);
    mdtTestTable.addDeltaCommit("20260101020101000");

    metaClient = HoodieTableMetaClient.reload(metaClient);
    mdtMetaClient = HoodieTableMetaClient.reload(mdtMetaClient);

    // Without MDT compaction, all rollback metadata is read — rolled-back commits appear
    Set<String> validTimestamps = HoodieTableMetadataUtil.getValidInstantTimestamps(metaClient, mdtMetaClient);
    assertTrue(validTimestamps.contains(commit1), "commit1 should be in valid timestamps");
    assertTrue(validTimestamps.contains(commit2), "commit2 should be in valid timestamps (from rollback metadata read)");
    assertTrue(validTimestamps.contains(commit3), "commit3 should be in valid timestamps (from rollback metadata read)");
    assertTrue(validTimestamps.contains(commit4), "commit4 should be in valid timestamps (from rollback metadata read)");
    assertTrue(validTimestamps.contains(commit5), "commit5 should be in valid timestamps");

    // Now add a compaction commit to MDT at a time between rollback2 and rollback3
    mdtTestTable.addCommit("20260401010101000");
    mdtTestTable.addDeltaCommit("20260501020101000");

    metaClient = HoodieTableMetaClient.reload(metaClient);
    mdtMetaClient = HoodieTableMetaClient.reload(mdtMetaClient);

    // With MDT compaction, only post-compaction rollback (commit4) metadata is read;
    // pre-compaction rollbacks for commit2 and commit3 are skipped
    validTimestamps = HoodieTableMetadataUtil.getValidInstantTimestamps(metaClient, mdtMetaClient);
    assertTrue(validTimestamps.contains(commit1), "commit1 should be in valid timestamps");
    assertTrue(validTimestamps.contains(commit5), "commit5 should be in valid timestamps");
    assertTrue(validTimestamps.contains(commit4), "commit4 should be in valid timestamps (from post-compaction rollback)");
    assertFalse(validTimestamps.contains(commit2), "commit2 should NOT be in valid timestamps (pre-compaction rollback skipped)");
    assertFalse(validTimestamps.contains(commit3), "commit3 should NOT be in valid timestamps (pre-compaction rollback skipped)");
  }

  private void addCompletedRollback(HoodieTestTable testTable, String rollbackTime, String rolledBackCommit) throws Exception {
    Map<String, List<String>> emptyPartitionFiles = new HashMap<>();
    emptyPartitionFiles.put("partition1", Collections.emptyList());
    HoodieRollbackMetadata rollbackMeta = testTable.getRollbackMetadata(rolledBackCommit, emptyPartitionFiles, false);
    HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan();
    rollbackPlan.setInstantToRollback(new HoodieInstantInfo(rolledBackCommit, HoodieTimeline.COMMIT_ACTION));
    rollbackPlan.setRollbackRequests(Collections.emptyList());
    testTable.addRollback(rollbackTime, rollbackMeta, rollbackPlan);
    testTable.addRollbackCompleted(rollbackTime, rollbackMeta, false);
  }

  private HoodieTableMetaClient createMdtMetaClient() throws IOException {
    String mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePathV2().toString());
    HoodieTestUtils.init(metaClient.getHadoopConf(), mdtBasePath, MERGE_ON_READ);
    return HoodieTableMetaClient.builder()
        .setConf(metaClient.getHadoopConf())
        .setBasePath(mdtBasePath)
        .build();
  }
}
