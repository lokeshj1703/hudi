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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.util.Option;

import static org.apache.hudi.common.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieMemoryConfig.DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE;

public class IOUtils {
  /**
   * Dynamic calculation of max memory to use for spillable map. There is always more than one task
   * running on an executor and each task maintains a spillable map.
   * user.available.memory = executor.memory * (1 - memory.fraction)
   * spillable.available.memory = user.available.memory * hoodie.memory.fraction / executor.cores.
   * Anytime the engine memory fractions/total memory is changed, the memory used for spillable map
   * changes accordingly.
   */
  public static long getMaxMemoryAllowedForMerge(TaskContextSupplier context, String maxMemoryFraction) {
    Option<String> totalMemoryOpt = context.getProperty(EngineProperty.TOTAL_MEMORY_AVAILABLE);
    Option<String> memoryFractionOpt = context.getProperty(EngineProperty.MEMORY_FRACTION_IN_USE);
    Option<String> totalCoresOpt = context.getProperty(EngineProperty.TOTAL_CORES_PER_EXECUTOR);

    if (totalMemoryOpt.isPresent() && memoryFractionOpt.isPresent() && totalCoresOpt.isPresent()) {
      long executorMemoryInBytes = Long.parseLong(totalMemoryOpt.get());
      double memoryFraction = Double.parseDouble(memoryFractionOpt.get());
      double maxMemoryFractionForMerge = Double.parseDouble(maxMemoryFraction);
      long executorCores = Long.parseLong(totalCoresOpt.get());
      double userAvailableMemory = executorMemoryInBytes * (1 - memoryFraction) / executorCores;
      long maxMemoryForMerge = (long) Math.floor(userAvailableMemory * maxMemoryFractionForMerge);
      return Math.max(DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES, maxMemoryForMerge);
    } else {
      return DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
    }
  }

  public static long getMaxMemoryPerPartitionMerge(TaskContextSupplier context, HoodieConfig hoodieConfig) {
    if (hoodieConfig.contains(MAX_MEMORY_FOR_MERGE)) {
      return hoodieConfig.getLong(MAX_MEMORY_FOR_MERGE);
    }
    String fraction = hoodieConfig.getStringOrDefault(MAX_MEMORY_FRACTION_FOR_MERGE);
    return getMaxMemoryAllowedForMerge(context, fraction);
  }

  public static long getMaxMemoryPerCompaction(TaskContextSupplier context, HoodieConfig hoodieConfig) {
    if (hoodieConfig.contains(MAX_MEMORY_FOR_COMPACTION)) {
      return hoodieConfig.getLong(MAX_MEMORY_FOR_COMPACTION);
    }
    String fraction = hoodieConfig.getStringOrDefault(MAX_MEMORY_FRACTION_FOR_COMPACTION);
    return getMaxMemoryAllowedForMerge(context, fraction);
  }

  /**
   * Returns {@code Option.of(per-task memory ceiling in bytes)} when the engine exposes
   * {@link EngineProperty#TOTAL_MEMORY_AVAILABLE}, {@link EngineProperty#MEMORY_FRACTION_IN_USE},
   * and {@link EngineProperty#TOTAL_CORES_PER_EXECUTOR} (Spark). Returns {@code Option.empty()}
   * otherwise (Flink, or Spark without {@code SparkEnv}) so callers can fall back to a static
   * config-driven cap rather than the spillable-map 1GB default.
   *
   * <p>Formula: {@code userAvailableMemory = totalExecutorMemory * (1 - memoryFraction) /
   * executorCores; return max(minFloor, floor(userAvailableMemory * fraction))}.
   *
   * <p>Distinct from {@link #getMaxMemoryAllowedForMerge} in two ways: returns {@code Option}
   * rather than collapsing the absent case to a default, and accepts the floor as a parameter
   * so callers (e.g., {@code HoodieAppendHandle}) can use a smaller floor than the spillable-map
   * 100MB.
   */
  public static Option<Long> getMaxMemoryAllowedForLogAppend(
      TaskContextSupplier context, String maxMemoryFraction, long minFloor) {
    Option<String> totalMemoryOpt = context.getProperty(EngineProperty.TOTAL_MEMORY_AVAILABLE);
    Option<String> memoryFractionOpt = context.getProperty(EngineProperty.MEMORY_FRACTION_IN_USE);
    Option<String> totalCoresOpt = context.getProperty(EngineProperty.TOTAL_CORES_PER_EXECUTOR);
    if (!(totalMemoryOpt.isPresent() && memoryFractionOpt.isPresent() && totalCoresOpt.isPresent())) {
      return Option.empty();
    }
    long executorMemoryInBytes = Long.parseLong(totalMemoryOpt.get());
    double memoryFraction = Double.parseDouble(memoryFractionOpt.get());
    double appendFraction = Double.parseDouble(maxMemoryFraction);
    long executorCores = Long.parseLong(totalCoresOpt.get());
    double userAvailableMemory = executorMemoryInBytes * (1 - memoryFraction) / executorCores;
    long ceiling = (long) Math.floor(userAvailableMemory * appendFraction);
    return Option.of(Math.max(minFloor, ceiling));
  }
}
