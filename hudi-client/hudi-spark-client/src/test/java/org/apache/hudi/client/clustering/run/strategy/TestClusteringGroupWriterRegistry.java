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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestClusteringGroupWriterRegistry {

  @AfterEach
  void clearOverride() {
    ClusteringGroupWriterRegistry.setOverrideForTesting(null);
  }

  @Test
  void getReturnsEmptyWhenNoProviderRegistered() {
    // No META-INF/services file is shipped in this module's test resources, so the
    // ServiceLoader resolves to nothing and the override slot is null.
    assertFalse(ClusteringGroupWriterRegistry.get().isPresent());
  }

  @Test
  void overrideIsReturnedWhenSet() {
    ClusteringGroupWriter stub = new StubGroupWriter("stub");
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.of(stub));
    assertTrue(ClusteringGroupWriterRegistry.get().isPresent());
    assertSame(stub, ClusteringGroupWriterRegistry.get().get());
  }

  @Test
  void clearingOverrideRestoresServiceLoaderResult() {
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.of(new StubGroupWriter("stub")));
    assertTrue(ClusteringGroupWriterRegistry.get().isPresent());

    ClusteringGroupWriterRegistry.setOverrideForTesting(null);
    assertFalse(ClusteringGroupWriterRegistry.get().isPresent());
  }

  @Test
  void emptyOverrideHidesServiceLoaderResult() {
    // Setting an explicit empty Option masks any ServiceLoader-resolved instance.
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.empty());
    assertFalse(ClusteringGroupWriterRegistry.get().isPresent());
  }

  @Test
  void overrideSurvivesAcrossRepeatedReads() {
    // Sanity check that AtomicReference.get() is idempotent and not consumed by reads —
    // covers the "current != null" branch of get() across multiple calls.
    StubGroupWriter writer = new StubGroupWriter("stable");
    ClusteringGroupWriterRegistry.setOverrideForTesting(Option.of(writer));
    for (int i = 0; i < 3; i++) {
      Option<ClusteringGroupWriter> result = ClusteringGroupWriterRegistry.get();
      assertTrue(result.isPresent());
      assertSame(writer, result.get());
    }
  }

  private static final class StubGroupWriter implements ClusteringGroupWriter {
    private final String name;

    StubGroupWriter(String name) {
      this.name = name;
    }

    @Override
    public Option<CompletableFuture<HoodieData<WriteStatus>>> runClusteringForGroupAsync(
        ClusteringGroupWriteContext context) {
      return Option.empty();
    }

    @Override
    public String name() {
      return name;
    }
  }
}
