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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the SPI gating in {@link SparkSortAndSizeExecutionStrategy}:
 *
 * <ul>
 *   <li>{@code shouldForceRowWriter()} only returns true when a registered
 *       {@link ClusteringGroupWriter} reports {@link ClusteringGroupWriter#isEnabled()}.</li>
 *   <li>{@code tryDelegateToGroupWriter()} returns the writer's future when a writer is
 *       registered, enabled, and can serve the group; otherwise empty so the caller falls
 *       back.</li>
 *   <li>{@code runClusteringForGroupAsyncAsRow} returns the writer's future when delegation
 *       succeeds, and routes to {@code super.runClusteringForGroupAsyncAsRow} when the
 *       writer returns empty.</li>
 * </ul>
 */
class TestSparkSortAndSizeExecutionStrategyAcceleration {

  private static JavaSparkContext jsc;

  @BeforeAll
  static void startSpark() {
    SparkConf conf = new SparkConf()
        .setAppName("TestSparkSortAndSizeExecutionStrategyAcceleration")
        .setMaster("local[1]");
    jsc = new JavaSparkContext(conf);
  }

  @AfterAll
  static void stopSpark() {
    if (jsc != null) {
      jsc.stop();
    }
  }

  @AfterEach
  void resetState() {
    ClusteringGroupWriterRegistry.setOverrideForTesting(null);
  }

  // ---------------------------------------------------------------------------------------
  // shouldForceRowWriter()
  // ---------------------------------------------------------------------------------------

  @Test
  void shouldForceRowWriterIsFalseWhenWriterDisabled() {
    ClusteringGroupWriterRegistry.setOverrideForTesting(
        Option.of(new RecordingGroupWriter(true, false)));
    assertFalse(newStrategy().shouldForceRowWriter());
  }

  @Test
  void shouldForceRowWriterIsFalseWhenNoWriterRegistered() {
    assertFalse(newStrategy().shouldForceRowWriter());
  }

  @Test
  void shouldForceRowWriterIsTrueWhenWriterEnabledAndRegistered() {
    ClusteringGroupWriterRegistry.setOverrideForTesting(
        Option.of(new RecordingGroupWriter(true, true)));
    assertTrue(newStrategy().shouldForceRowWriter());
  }

  // ---------------------------------------------------------------------------------------
  // tryDelegateToGroupWriter()
  // ---------------------------------------------------------------------------------------

  @Test
  void tryDelegateReturnsEmptyWhenWriterDisabled() {
    ClusteringGroupWriterRegistry.setOverrideForTesting(
        Option.of(new RecordingGroupWriter(true, false)));
    assertFalse(newStrategy().tryDelegateToGroupWriter(
        new HoodieClusteringGroup(), Collections.emptyMap(), true, "001",
        mock(ExecutorService.class)).isPresent());
  }

  @Test
  void tryDelegateReturnsEmptyWhenNoWriterRegistered() {
    assertFalse(newStrategy().tryDelegateToGroupWriter(
        new HoodieClusteringGroup(), Collections.emptyMap(), true, "001",
        mock(ExecutorService.class)).isPresent());
  }

  @Test
  void tryDelegateReturnsEmptyWhenWriterCannotServeGroup() {
    RecordingGroupWriter writer = new RecordingGroupWriter(false, true);
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.of(writer));

    Option<CompletableFuture<HoodieData<WriteStatus>>> result = newStrategy().tryDelegateToGroupWriter(
        new HoodieClusteringGroup(), Collections.emptyMap(), true, "001",
        mock(ExecutorService.class));

    assertEquals(1, writer.invocations.get());
    assertFalse(result.isPresent());
  }

  @Test
  void tryDelegateReturnsWriterFutureWhenItServesGroup() {
    RecordingGroupWriter writer = new RecordingGroupWriter(true, true);
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.of(writer));

    Option<CompletableFuture<HoodieData<WriteStatus>>> result = newStrategy().tryDelegateToGroupWriter(
        new HoodieClusteringGroup(), Collections.emptyMap(), true, "001",
        mock(ExecutorService.class));

    assertEquals(1, writer.invocations.get());
    assertTrue(result.isPresent());
    assertSame(writer.lastResult, result.get());
  }

  // ---------------------------------------------------------------------------------------
  // runClusteringForGroupAsyncAsRow() — production override
  // ---------------------------------------------------------------------------------------

  @Test
  void runClusteringForGroupAsyncAsRowReturnsWriterFutureOnDelegation() {
    RecordingGroupWriter writer = new RecordingGroupWriter(true, true);
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.of(writer));

    CompletableFuture<HoodieData<WriteStatus>> result = newStrategy().runClusteringForGroupAsyncAsRow(
        new HoodieClusteringGroup(), Collections.emptyMap(), true, "001",
        mock(ExecutorService.class));

    assertSame(writer.lastResult, result);
  }

  /**
   * When the writer returns {@link Option#empty()}, the override must fall through to
   * {@code super.runClusteringForGroupAsyncAsRow}. We can't drive the real super-call
   * (it needs a fully-bootstrapped HoodieTable) so we use a counting subclass that stubs
   * super. This anchors the routing contract in this repo's CI even though the true
   * super-call path is exercised end-to-end by the integration clustering test.
   */
  @Test
  void runClusteringForGroupAsyncAsRowFallsBackToSuperWhenWriterReturnsEmpty() {
    RecordingGroupWriter writer = new RecordingGroupWriter(false, true);
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.of(writer));

    SuperCountingStrategy strategy = newSuperCountingStrategy();
    CompletableFuture<HoodieData<WriteStatus>> result = strategy.runClusteringForGroupAsyncAsRow(
        new HoodieClusteringGroup(), Collections.emptyMap(), true, "001",
        mock(ExecutorService.class));

    assertEquals(1, writer.invocations.get(), "writer must be consulted");
    assertEquals(1, strategy.superInvocations.get(), "super.runClusteringForGroupAsyncAsRow must run on fallback");
    assertSame(strategy.superResult, result, "fallback returns the super-class result");
  }

  // ---------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------

  private SparkSortAndSizeExecutionStrategy<Object> newStrategy() {
    HoodieEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = newWriteConfig();
    return new SparkSortAndSizeExecutionStrategy<>(mockTable(writeConfig), engineContext, writeConfig);
  }

  private SuperCountingStrategy newSuperCountingStrategy() {
    HoodieEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = newWriteConfig();
    return new SuperCountingStrategy(mockTable(writeConfig), engineContext, writeConfig);
  }

  private static HoodieWriteConfig newWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi-acceleration-test")
        .withSchema(MINIMAL_RECORD_SCHEMA)
        .withProperties(new java.util.Properties())
        .build();
  }

  /** Single home for the unchecked cast on the mock {@link HoodieTable}. */
  @SuppressWarnings("unchecked")
  private static HoodieTable<Object, Object, Object, Object> mockTable(HoodieWriteConfig writeConfig) {
    HoodieTable<Object, Object, Object, Object> table =
        (HoodieTable<Object, Object, Object, Object>) mock(HoodieTable.class);
    when(table.getConfig()).thenReturn(writeConfig);
    return table;
  }

  private static final String MINIMAL_RECORD_SCHEMA =
      "{\"type\":\"record\",\"name\":\"AccelerationTestRecord\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

  /**
   * Subclass that overrides ONLY {@code runSuperRunClusteringForGroupAsyncAsRow} (the
   * indirection over {@code super}), so the production {@code runClusteringForGroupAsyncAsRow}
   * routing runs unchanged. Counts invocations and returns a stable future so the fallback
   * contract can be asserted without bootstrapping a real HoodieTable.
   */
  private static final class SuperCountingStrategy extends SparkSortAndSizeExecutionStrategy<Object> {
    final AtomicInteger superInvocations = new AtomicInteger();
    final CompletableFuture<HoodieData<WriteStatus>> superResult = new CompletableFuture<>();

    SuperCountingStrategy(HoodieTable<Object, Object, Object, Object> table,
                          HoodieEngineContext engineContext,
                          HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    @Override
    CompletableFuture<HoodieData<WriteStatus>> runSuperRunClusteringForGroupAsyncAsRow(
        HoodieClusteringGroup clusteringGroup,
        Map<String, String> strategyParams,
        boolean shouldPreserveHoodieMetadata,
        String instantTime,
        ExecutorService clusteringExecutorService) {
      superInvocations.incrementAndGet();
      return superResult;
    }
  }

  /** Records invocations + serves either a stable future or empty; gated by an isEnabled flag. */
  private static final class RecordingGroupWriter implements ClusteringGroupWriter {
    final AtomicInteger invocations = new AtomicInteger();
    final boolean serves;
    final boolean enabled;
    final CompletableFuture<HoodieData<WriteStatus>> lastResult = new CompletableFuture<>();

    RecordingGroupWriter(boolean serves, boolean enabled) {
      this.serves = serves;
      this.enabled = enabled;
    }

    @Override
    public Option<CompletableFuture<HoodieData<WriteStatus>>> runClusteringForGroupAsync(
        ClusteringGroupWriteContext context) {
      invocations.incrementAndGet();
      return serves ? Option.of(lastResult) : Option.empty();
    }

    @Override
    public String name() {
      return "recording";
    }

    @Override
    public boolean isEnabled() {
      return enabled;
    }
  }
}
