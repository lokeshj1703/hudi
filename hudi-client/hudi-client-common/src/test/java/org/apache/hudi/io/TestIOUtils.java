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

import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IOUtils#getMaxMemoryAllowedForLogAppend} — the dynamic per-task memory
 * helper used by {@code HoodieAppendHandle} to cap its in-memory log-block buffer.
 *
 * <p>Distinct from the {@link TestHoodieAppendHandle} cases, which exercise the helper via the
 * handle. These tests pin the helper's contract directly: absent-engine-props → empty option,
 * formula, and parameterized floor.
 */
public class TestIOUtils {

  private static final String DEFAULT_FRACTION = "0.6";
  private static final long FLOOR_16MB = 16L * 1024 * 1024;

  /**
   * When the engine does not expose memory/cores (Flink {@code FlinkTaskContextSupplier} returns
   * {@code Option.empty()} unconditionally; Spark also returns empty if {@code SparkEnv} is
   * absent), the helper must return {@code Option.empty()} so callers can fall back to a
   * static config-driven cap rather than the spillable-map 1GB default.
   */
  @Test
  void testReturnsEmptyWhenAnyEnginePropertyMissing() {
    assertFalse(IOUtils.getMaxMemoryAllowedForLogAppend(
        new StubTaskContextSupplier(null, null, null), DEFAULT_FRACTION, FLOOR_16MB).isPresent());
    assertFalse(IOUtils.getMaxMemoryAllowedForLogAppend(
        new StubTaskContextSupplier(1024L, null, 1), DEFAULT_FRACTION, FLOOR_16MB).isPresent());
    assertFalse(IOUtils.getMaxMemoryAllowedForLogAppend(
        new StubTaskContextSupplier(1024L, 0.6, null), DEFAULT_FRACTION, FLOOR_16MB).isPresent());
    assertFalse(IOUtils.getMaxMemoryAllowedForLogAppend(
        new StubTaskContextSupplier(null, 0.6, 1), DEFAULT_FRACTION, FLOOR_16MB).isPresent());
  }

  /**
   * Formula check on a "normal" Spark executor: 4GB / 0.6 memory-fraction / 4 cores
   * = 400MB user memory per task, * 0.6 append-fraction = 240MB ceiling.
   */
  @Test
  void testComputesPerTaskCeilingFromEngineProperties() {
    long executorBytes = 4L * 1024 * 1024 * 1024;
    Option<Long> ceiling = IOUtils.getMaxMemoryAllowedForLogAppend(
        new StubTaskContextSupplier(executorBytes, 0.6, 4),
        DEFAULT_FRACTION,
        FLOOR_16MB);

    assertTrue(ceiling.isPresent());
    long expected = (long) Math.floor(executorBytes * (1 - 0.6) / 4 * 0.6);
    assertEquals(expected, ceiling.get().longValue());
  }

  /**
   * When the raw computation falls below the supplied floor, the floor wins. Mirrors the
   * "tiny executor" path that the 16MB floor protects against on production small-executor
   * deployments.
   */
  @Test
  void testRespectsConfigurableFloor() {
    long executorBytes = 50L * 1024 * 1024;
    Option<Long> ceiling = IOUtils.getMaxMemoryAllowedForLogAppend(
        new StubTaskContextSupplier(executorBytes, 0.6, 1),
        DEFAULT_FRACTION,
        FLOOR_16MB);

    assertTrue(ceiling.isPresent());
    long rawComputed = (long) Math.floor(executorBytes * (1 - 0.6) / 1 * 0.6);
    assertTrue(rawComputed < FLOOR_16MB, "test setup must drive computation below floor");
    assertEquals(FLOOR_16MB, ceiling.get().longValue());
  }

  /**
   * The floor is genuinely parameterized — callers can choose a different lower bound. Pin this
   * so the merge/compaction call sites (which use the 100MB spillable floor via the sibling
   * helper) and the log-append call site (16MB) cannot collide on a shared constant.
   */
  @Test
  void testFloorIsParameterized() {
    long executorBytes = 50L * 1024 * 1024;
    long floor1MB = 1024 * 1024L;
    Option<Long> ceiling = IOUtils.getMaxMemoryAllowedForLogAppend(
        new StubTaskContextSupplier(executorBytes, 0.6, 1),
        DEFAULT_FRACTION,
        floor1MB);

    assertTrue(ceiling.isPresent());
    long rawComputed = (long) Math.floor(executorBytes * (1 - 0.6) / 1 * 0.6); // 12MB
    assertEquals(rawComputed, ceiling.get().longValue(),
        "with a 1MB floor, the raw computation (12MB) wins");
  }

  /**
   * The fraction parameter controls how aggressive the cap is. A 0.3 fraction on the same
   * executor yields half the ceiling that 0.6 would.
   */
  @Test
  void testFractionControlsCeilingMagnitude() {
    long executorBytes = 4L * 1024 * 1024 * 1024;
    StubTaskContextSupplier supplier = new StubTaskContextSupplier(executorBytes, 0.6, 4);

    Option<Long> half = IOUtils.getMaxMemoryAllowedForLogAppend(supplier, "0.3", FLOOR_16MB);
    Option<Long> full = IOUtils.getMaxMemoryAllowedForLogAppend(supplier, "0.6", FLOOR_16MB);

    assertTrue(half.isPresent() && full.isPresent());
    assertEquals(full.get() / 2, half.get().longValue(),
        "halving the fraction halves the ceiling on a generous executor");
  }

  /**
   * {@link TaskContextSupplier} stub returning a configurable response for each of the three
   * engine properties the formula consumes. {@code null} signals "engine did not expose this
   * property" (which maps to {@link Option#empty()}).
   */
  private static final class StubTaskContextSupplier extends TaskContextSupplier {
    private final Long executorMemoryBytes;
    private final Double memoryFraction;
    private final Integer executorCores;

    StubTaskContextSupplier(Long executorMemoryBytes, Double memoryFraction, Integer executorCores) {
      this.executorMemoryBytes = executorMemoryBytes;
      this.memoryFraction = memoryFraction;
      this.executorCores = executorCores;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      switch (prop) {
        case TOTAL_MEMORY_AVAILABLE:
          return executorMemoryBytes == null ? Option.empty() : Option.of(String.valueOf(executorMemoryBytes));
        case MEMORY_FRACTION_IN_USE:
          return memoryFraction == null ? Option.empty() : Option.of(String.valueOf(memoryFraction));
        case TOTAL_CORES_PER_EXECUTOR:
          return executorCores == null ? Option.empty() : Option.of(String.valueOf(executorCores));
        default:
          return Option.empty();
      }
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> 0;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return () -> 0;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return () -> 0L;
    }

    @Override
    public Supplier<Integer> getTaskAttemptNumberSupplier() {
      return () -> -1;
    }

    @Override
    public Supplier<Integer> getStageAttemptNumberSupplier() {
      return () -> -1;
    }
  }
}
