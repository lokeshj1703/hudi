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

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieBackedTableMetadataWriter {

  private static FileStatus createMetadataFileStatus() {
    FileStatus status = mock(FileStatus.class);
    Path path = mock(Path.class);
    String fullName = HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX;
    when(path.getName()).thenReturn(fullName);
    when(status.getPath()).thenReturn(path);
    return status;
  }

  private static FileStatus createFileStatus(String groupName, String splitTwo, String commitTime, String extension) {
    FileStatus status = mock(FileStatus.class);
    Path path = mock(Path.class);
    String fullName = groupName + "_" + splitTwo + "_" + commitTime + extension;
    when(path.getName()).thenReturn(fullName);
    when(status.getPath()).thenReturn(path);
    return status;
  }

  private static Stream<Arguments> generateFileStatusArguments() {
    final String PARQ_EXT = HoodieFileFormat.PARQUET.getFileExtension();
    final String HFILE_EXT = HoodieFileFormat.HFILE.getFileExtension();
    final String ORC_EXT = HoodieFileFormat.ORC.getFileExtension();
    final String LOG_EXT = HoodieFileFormat.HOODIE_LOG.getFileExtension();
    return Stream.of(
        // two different parquet files = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file2", "diff", "01", PARQ_EXT)
        }, false),
        // two parquet files with same group different commit = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "same", "01", PARQ_EXT),
            createFileStatus("file1", "same", "02", PARQ_EXT)
        }, false),
        // two parquet files with same file group same commit = dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", PARQ_EXT)
        }, true),
        // parquet/hfile with same file group same commit = dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", HFILE_EXT)
        }, true),
        // parquet/orc with same file group same commit = dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", ORC_EXT)
        }, true),
        // parquet/log with same file group same commit = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", LOG_EXT)
        }, false),
        // log/log with same file group same commit = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", LOG_EXT),
            createFileStatus("file1", "diff", "01", LOG_EXT)
        }, false)
    );
  }

  @ParameterizedTest
  @MethodSource("generateFileStatusArguments")
  public void testDirectoryInfoThrowsErrorForDupeNameCommitPairs(FileStatus[] fileStatuses, boolean expectError) {
    if (expectError) {
      assertThrows(HoodieIOException.class,
          () -> new HoodieBackedTableMetadataWriter.DirectoryInfo("any", fileStatuses, "999999")
      );
    } else {
      assertDoesNotThrow(
          () -> new HoodieBackedTableMetadataWriter.DirectoryInfo("any", fileStatuses, "999999")
      );
    }
  }

  @Test
  void rollbackFailedWrites_reloadsTimelineOnWritesRolledBack() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(true);
    try (MockedStatic<HoodieTableMetaClient> mockedStatic = mockStatic(HoodieTableMetaClient.class)) {
      HoodieTableMetaClient reloadedClient = mock(HoodieTableMetaClient.class);
      mockedStatic.when(() -> HoodieTableMetaClient.reload(mockMetaClient)).thenReturn(reloadedClient);
      assertSame(reloadedClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(writeConfig, mockWriteClient, mockMetaClient));
    }
  }

  @Test
  void rollbackFailedWrites_avoidsTimelineReload() {
    HoodieWriteConfig eagerWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    BaseHoodieWriteClient mockWriteClient = mock(BaseHoodieWriteClient.class);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockWriteClient.rollbackFailedWrites(mockMetaClient)).thenReturn(false);
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(eagerWriteConfig, mockWriteClient, mockMetaClient));

    HoodieWriteConfig lazyWriteConfig = HoodieWriteConfig.newBuilder().withPath("file://tmp/")
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .build();
    assertSame(mockMetaClient, HoodieBackedTableMetadataWriter.rollbackFailedWrites(lazyWriteConfig, mockWriteClient, mockMetaClient));
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true,true,false,true",
      "false,true,false,true",
      "true,false,false,true",
      "false,false,false,false",
      "false,false,true,false",
  })
  void runPendingTableServicesOperations(boolean hasPendingCompaction, boolean hasPendingLogCompaction, boolean requiresRefresh, boolean ranService) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline initialTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);
    if (requiresRefresh) {
      when(metaClient.reloadActiveTimeline()).thenReturn(initialTimeline);
    } else {
      when(metaClient.getActiveTimeline()).thenReturn(initialTimeline);
    }
    if (hasPendingCompaction) {
      when(initialTimeline.filterPendingCompactionTimeline().countInstants()).thenReturn(1);
    }
    if (hasPendingLogCompaction) {
      when(initialTimeline.filterPendingLogCompactionTimeline().countInstants()).thenReturn(1);
    }
    HoodieActiveTimeline expectedResult;
    if (ranService) {
      HoodieActiveTimeline timelineReloadedAfterServicesRun = mock(HoodieActiveTimeline.class);
      when(metaClient.reloadActiveTimeline()).thenReturn(timelineReloadedAfterServicesRun);
      expectedResult = timelineReloadedAfterServicesRun;
    } else {
      expectedResult = initialTimeline;
    }
    assertSame(expectedResult, HoodieBackedTableMetadataWriter.runPendingTableServicesOperationsAndRefreshTimeline(metaClient, writeClient, requiresRefresh));

    verify(writeClient, times(hasPendingCompaction ? 1 : 0)).runAnyPendingCompactions();
    verify(writeClient, times(hasPendingLogCompaction ? 1 : 0)).runAnyPendingLogCompactions();
    int expectedTimelineReloads = (requiresRefresh ? 1 : 0) + (ranService ? 1 : 0);
    verify(metaClient, times(expectedTimelineReloads)).reloadActiveTimeline();
  }

  // ---- ensureSchemaForRLIBootstrap tests ----

  private static final String SIMPLE_SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"Test\",\"namespace\":\"test\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

  @Test
  void ensureSchemaForRLIBootstrap_noopWhenSchemaPresent() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getSchema()).thenReturn(SIMPLE_SCHEMA_JSON);
    assertDoesNotThrow(() -> HoodieBackedTableMetadataWriter.ensureSchemaForRLIBootstrap(metaClient, writeConfig));
  }

  @Test
  void ensureSchemaForRLIBootstrap_fallsBackToTableSchemaResolverWhenNull() throws Exception {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getSchema()).thenReturn(null);

    Schema tableSchema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
    try (org.mockito.MockedConstruction<TableSchemaResolver> mockedResolver =
        mockConstruction(TableSchemaResolver.class,
            (resolver, ctx) -> when(resolver.getTableAvroSchema(false)).thenReturn(tableSchema))) {

      HoodieBackedTableMetadataWriter.ensureSchemaForRLIBootstrap(metaClient, writeConfig);

      assertEquals(1, mockedResolver.constructed().size());
      verify(writeConfig).setValue(HoodieWriteConfig.AVRO_SCHEMA_STRING, tableSchema.toString());
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
      "true,0",   // clean instant already exists, clean should not be called
      "false,1"   // clean instant does not exist, clean should be called once
  })
  void cleanIfNecessary_skipsDuplicateCleanInstants(boolean cleanInstantExists, int expectedCleanCalls,
                                                     @TempDir java.nio.file.Path tempDir) throws Exception {
    String basePath = tempDir.toString();
    String instantTime = "20230101120000";
    String cleanInstant = "20230101120000999"; // HoodieTableMetadataUtil.createCleanTimestamp format

    // Create a real Hudi table
    Configuration hadoopConf = new Configuration();
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .setPayloadClassName(org.apache.hudi.common.model.HoodieAvroPayload.class.getName())
        .initTable(hadoopConf, basePath);

    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class, RETURNS_DEEP_STUBS);
    BaseHoodieWriteClient writeClient = mock(BaseHoodieWriteClient.class);

    // Mock the timeline structure for metadata metaClient
    when(metadataMetaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(metadataMetaClient.getCommitTimeline().filterCompletedInstants().lastInstant())
        .thenReturn(Option.empty()); // No recent compaction to skip clean

    // Mock the cleaner timeline check
    when(activeTimeline.getCleanerTimeline().filterCompletedInstants().containsInstant(cleanInstant))
        .thenReturn(cleanInstantExists);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.createCleanTimestamp(instantTime))
          .thenReturn(cleanInstant);

      // Create a concrete test implementation
      TestableHoodieBackedTableMetadataWriter writer = new TestableHoodieBackedTableMetadataWriter(basePath, metadataMetaClient);

      // Execute
      writer.cleanIfNecessary(writeClient, instantTime);

      // Verify
      verify(writeClient, times(expectedCleanCalls)).clean(cleanInstant);
      verify(writeClient, times(1)).lazyRollbackFailedIndexing();
    }
  }

  /**
   * Test implementation of HoodieBackedTableMetadataWriter to expose protected methods for testing.
   */
  private static class TestableHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<Object> {
    private final HoodieTableMetaClient metadataMetaClient;

    TestableHoodieBackedTableMetadataWriter(String basePath, HoodieTableMetaClient metadataMetaClient) {
      super(
          new Configuration(),
          HoodieWriteConfig.newBuilder().withPath(basePath).build(),
          HoodieFailedWritesCleaningPolicy.EAGER,
          null, // engineContext not needed for this test
          Option.empty()
      );
      this.metadataMetaClient = metadataMetaClient;
    }

    @Override
    protected boolean initializeIfNeeded(HoodieTableMetaClient dataMetaClient,
                                         Option<String> inflightInstantTimestamp) throws IOException {
      // Return false to avoid initialization and keep this.metadata null
      return false;
    }

    @Override
    protected HoodieTableMetaClient getMetadataMetaClient() {
      return metadataMetaClient;
    }

    @Override
    protected HoodieTable getHoodieTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) {
      return null;
    }

    @Override
    protected void initRegistry() {
      // No-op for testing
    }

    @Override
    public void close() throws Exception {
      // No-op for testing
    }

    @Override
    protected void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap) {
      // No-op for testing
    }

    @Override
    protected Object convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records) {
      return null;
    }

    @Override
    protected void bulkCommit(String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records, MetadataTableFileGroupIndexParser indexParser) {
      // No-op for testing
    }

    @Override
    protected BaseHoodieWriteClient<?, Object, ?, ?> initializeWriteClient() {
      return null;
    }

    @Override
    public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
      // No-op for testing
    }
  }
}
