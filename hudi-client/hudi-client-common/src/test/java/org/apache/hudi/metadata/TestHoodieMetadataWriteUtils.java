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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMetadataWriteUtils {

  @Test
  public void testCreateMetadataWriteConfigForCleaner() {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.CLEANER_PARALLELISM.key(), "1000");
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withProperties(properties).build())
        .build();

    HoodieWriteConfig metadataWriteConfig1 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig1, HoodieFailedWritesCleaningPolicy.EAGER);
    assertEquals(HoodieFailedWritesCleaningPolicy.EAGER, metadataWriteConfig1.getFailedWritesCleanPolicy());
    assertEquals(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, metadataWriteConfig1.getCleanerPolicy());
    // default value already greater than data cleaner commits retained * 1.2
    assertEquals(HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED, metadataWriteConfig1.getCleanerCommitsRetained());
    assertEquals(1000, metadataWriteConfig1.getCleanerParallelism());

    assertNotEquals(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS, metadataWriteConfig1.getCleanerPolicy());
    assertNotEquals(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS, metadataWriteConfig1.getCleanerPolicy());

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(20).build())
        .build();
    HoodieWriteConfig metadataWriteConfig2 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig2, HoodieFailedWritesCleaningPolicy.EAGER);
    assertEquals(HoodieFailedWritesCleaningPolicy.EAGER, metadataWriteConfig2.getFailedWritesCleanPolicy());
    assertEquals(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, metadataWriteConfig2.getCleanerPolicy());
    // data cleaner commits retained * 1.2 is greater than default
    assertEquals(24, metadataWriteConfig2.getCleanerCommitsRetained());
  }

  @Test
  public void testParallelismConfigs() {
    // default
    testMetadataConfig(false, 512, 512, 512);
    // overrides
    testMetadataConfig(true, 10, 20, 10);
    testMetadataConfig(true, 1000, 2000, 1000);
  }

  private void testMetadataConfig(boolean setParallelismConfigs, int cleanerParallelism, int rollbackParallelism, int finalizeWriteParallelism) {
    Properties properties = new Properties();
    if (setParallelismConfigs) {
      properties.setProperty(HoodieMetadataConfig.CLEANER_PARALLELISM.key(), Integer.toString(cleanerParallelism));
      properties.setProperty(HoodieMetadataConfig.ROLLBACK_PARALLELISM.key(), Integer.toString(rollbackParallelism));
      properties.setProperty(HoodieMetadataConfig.FINALIZE_WRITES_PARALLELISM.key(), Integer.toString(finalizeWriteParallelism));
    }
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withProperties(properties).build())
        .build();
    HoodieWriteConfig metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, HoodieFailedWritesCleaningPolicy.EAGER);

    assertEquals(cleanerParallelism, metadataWriteConfig.getCleanerParallelism());
    assertEquals(rollbackParallelism, metadataWriteConfig.getRollbackParallelism());
    assertEquals(finalizeWriteParallelism, metadataWriteConfig.getFinalizeWriteParallelism());
  }

  @Test
  public void testFileSliceCache() {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.ENABLE_FILE_SLICE_CACHE_OPTIMIZATION.key(), "true");
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withProperties(properties).build())
        .build();

    HoodieWriteConfig metadataWriteConfig1 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig1, HoodieFailedWritesCleaningPolicy.EAGER);
    assertTrue(metadataWriteConfig1.shouldEnableFileSliceCacheOptimization());
    assertEquals(HoodieMetadataConfig.FILE_SLICE_CACHE_MAX_SIZE.defaultValue(), metadataWriteConfig1.getFileSliceCacheMaxSize());
    assertEquals(HoodieMetadataConfig.FILE_SLICE_CACHE_EXPIRATION_MINS.defaultValue(), metadataWriteConfig1.getFileSliceCacheExpirationInMins());

    properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.ENABLE_FILE_SLICE_CACHE_OPTIMIZATION.key(), "true");
    properties.setProperty(HoodieMetadataConfig.FILE_SLICE_CACHE_MAX_SIZE.key(), "10");
    properties.setProperty(HoodieMetadataConfig.FILE_SLICE_CACHE_EXPIRATION_MINS.key(), "20");
    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withProperties(properties).build())
        .build();

    HoodieWriteConfig metadataWriteConfig2 = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig2, HoodieFailedWritesCleaningPolicy.EAGER);
    assertTrue(metadataWriteConfig2.shouldEnableFileSliceCacheOptimization());
    assertEquals(10, metadataWriteConfig2.getFileSliceCacheMaxSize());
    assertEquals(20, metadataWriteConfig2.getFileSliceCacheExpirationInMins());
  }

  @Test
  public void testEmbeddedTimelineServerConfig() {
    // Test default (disabled)
    HoodieWriteConfig writeConfigDefault = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .build();
    HoodieWriteConfig metadataWriteConfigDefault = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfigDefault, HoodieFailedWritesCleaningPolicy.EAGER);
    assertFalse(metadataWriteConfigDefault.isEmbeddedTimelineServerEnabled(),
        "Embedded timeline server should be disabled by default for metadata table");

    // Test with embedded timeline server enabled
    Properties propertiesEnabled = new Properties();
    propertiesEnabled.setProperty(HoodieMetadataConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key(), "true");
    HoodieWriteConfig writeConfigEnabled = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withProperties(propertiesEnabled).build())
        .build();
    HoodieWriteConfig metadataWriteConfigEnabled = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfigEnabled, HoodieFailedWritesCleaningPolicy.EAGER);
    assertTrue(metadataWriteConfigEnabled.isEmbeddedTimelineServerEnabled(),
        "Embedded timeline server should be enabled for metadata table when configured");

    // Test with embedded timeline server explicitly disabled
    Properties propertiesDisabled = new Properties();
    propertiesDisabled.setProperty(HoodieMetadataConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key(), "false");
    HoodieWriteConfig writeConfigDisabled = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withProperties(propertiesDisabled).build())
        .build();
    HoodieWriteConfig metadataWriteConfigDisabled = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        writeConfigDisabled, HoodieFailedWritesCleaningPolicy.EAGER);
    assertFalse(metadataWriteConfigDisabled.isEmbeddedTimelineServerEnabled(),
        "Embedded timeline server should be disabled for metadata table when explicitly configured to false");
  }
}
