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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestHoodieRowMergeHandleConstruction extends HoodieCommonTestHarness {

  private static final String DEFAULT_PARTITION_PATH = "partition";
  private static final String DEFAULT_FILE_ID = "fileId";
  private static final String DEFAULT_INSTANT_TIME = "0000002";
  private static final String NEXT_INSTANT_TIME = "0000005";
  private static final String DEFAULT_FILE_NAME = String.format("%s_0-0-0_%s.parquet", DEFAULT_FILE_ID, DEFAULT_INSTANT_TIME);

  @Mock
  private HoodieTable mockTable;

  @Mock
  private TableFileSystemView.BaseFileOnlyView mockFileSystemView;

  @Mock
  private HoodieBaseFile mockBaseFile;

  @BeforeEach
  void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    initPath();
    initMetaClient();

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    when(mockTable.getMetaClient()).thenReturn(metaClient);
    when(mockTable.getConfig()).thenReturn(writeConfig);
    when(mockTable.getBaseFileOnlyView()).thenReturn(mockFileSystemView);
    when(mockTable.getPartitionMetafileFormat()).thenReturn(Option.empty());
    when(mockTable.getBaseFileExtension()).thenReturn(HoodieFileFormat.PARQUET.getFileExtension());
    when(mockTable.shouldTrackSuccessRecords()).thenReturn(false);
    when(mockTable.getHadoopConf()).thenReturn(metaClient.getHadoopConf());
    when(mockFileSystemView.getLatestBaseFile(DEFAULT_PARTITION_PATH, DEFAULT_FILE_ID)).thenReturn(Option.of(mockBaseFile));
    when(mockBaseFile.getFileName()).thenReturn(DEFAULT_FILE_NAME);
    when(mockBaseFile.getFileId()).thenReturn(DEFAULT_FILE_ID);
    when(mockBaseFile.getCommitTime()).thenReturn(DEFAULT_INSTANT_TIME);
  }

  @AfterEach
  void clean() {
    cleanMetaClient();
  }

  @Test
  void testRowMergeHandleConstructorWithMergeContext() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();
    when(mockTable.getConfig()).thenReturn(writeConfig);
    TaskContextSupplier taskContextSupplier = new LocalTaskContextSupplier();
    long expectedNumUpdates = 42L;
    Iterator emptyItr = Collections.emptyIterator();
    MergeContext<HoodieRecord> mergeContext = MergeContext.create(expectedNumUpdates, emptyItr);

    try (MockedStatic<WriteMarkersFactory> mockedMarkers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> mockedWriterFactory = mockStatic(HoodieFileWriterFactory.class)) {

      WriteMarkers mockWriteMarkers = mock(WriteMarkers.class);
      mockedMarkers.when(() -> WriteMarkersFactory.get(any(), any(), anyString())).thenReturn(mockWriteMarkers);

      HoodieFileWriter mockWriter = mock(HoodieFileWriter.class);
      mockedWriterFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
          anyString(), any(), any(), any(), any(), any(), any())).thenReturn(mockWriter);

      HoodieRowMergeHandle handle = new HoodieRowMergeHandle(
          writeConfig, NEXT_INSTANT_TIME, mockTable, mergeContext,
          DEFAULT_PARTITION_PATH, DEFAULT_FILE_ID,
          taskContextSupplier, mockBaseFile, Option.empty());

      assertNotNull(handle);
      assertEquals(expectedNumUpdates, handle.getNumIncomingUpdates());
    }
  }

  @Test
  void testRowMergeHandleFirstConstructorWithMergeContext() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();
    when(mockTable.getConfig()).thenReturn(writeConfig);
    TaskContextSupplier taskContextSupplier = new LocalTaskContextSupplier();
    long expectedNumUpdates = 100L;
    MergeContext<HoodieRecord> mergeContext = MergeContext.create(expectedNumUpdates, Collections.emptyIterator());

    try (MockedStatic<WriteMarkersFactory> mockedMarkers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> mockedWriterFactory = mockStatic(HoodieFileWriterFactory.class)) {

      WriteMarkers mockWriteMarkers = mock(WriteMarkers.class);
      mockedMarkers.when(() -> WriteMarkersFactory.get(any(), any(), anyString())).thenReturn(mockWriteMarkers);

      HoodieFileWriter mockWriter = mock(HoodieFileWriter.class);
      mockedWriterFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
          anyString(), any(), any(), any(), any(), any(), any())).thenReturn(mockWriter);

      HoodieRowMergeHandle handle = new HoodieRowMergeHandle(
          writeConfig, NEXT_INSTANT_TIME, mockTable, mergeContext,
          DEFAULT_PARTITION_PATH, DEFAULT_FILE_ID,
          taskContextSupplier, Option.empty());

      assertNotNull(handle);
      assertEquals(expectedNumUpdates, handle.getNumIncomingUpdates());
    }
  }

  @Test
  void testRowConcatHandleConstructorWithMergeContext() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();
    when(mockTable.getConfig()).thenReturn(writeConfig);
    TaskContextSupplier taskContextSupplier = new LocalTaskContextSupplier();
    long expectedNumUpdates = 55L;
    MergeContext<HoodieRecord> mergeContext = MergeContext.create(expectedNumUpdates, Collections.emptyIterator());

    try (MockedStatic<WriteMarkersFactory> mockedMarkers = mockStatic(WriteMarkersFactory.class);
         MockedStatic<HoodieFileWriterFactory> mockedWriterFactory = mockStatic(HoodieFileWriterFactory.class)) {

      WriteMarkers mockWriteMarkers = mock(WriteMarkers.class);
      mockedMarkers.when(() -> WriteMarkersFactory.get(any(), any(), anyString())).thenReturn(mockWriteMarkers);

      HoodieFileWriter mockWriter = mock(HoodieFileWriter.class);
      mockedWriterFactory.when(() -> HoodieFileWriterFactory.getFileWriter(
          anyString(), any(), any(), any(), any(), any(), any())).thenReturn(mockWriter);

      HoodieRowConcatHandle handle = new HoodieRowConcatHandle(
          writeConfig, NEXT_INSTANT_TIME, mockTable, mergeContext,
          DEFAULT_PARTITION_PATH, DEFAULT_FILE_ID,
          taskContextSupplier, Option.empty());

      assertNotNull(handle);
      assertEquals(expectedNumUpdates, handle.getNumIncomingUpdates());
    }
  }
}
