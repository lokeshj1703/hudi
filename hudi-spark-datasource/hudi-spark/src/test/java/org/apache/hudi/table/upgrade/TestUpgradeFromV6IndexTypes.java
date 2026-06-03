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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates that the 1.1 binary correctly reads and writes Hudi 0.14 (table version 6) MOR
 * tables created with different index types, and that each index continues to function
 * correctly — specifically that upserts locate existing records (no phantom duplicates).
 *
 * <p>Fixtures: {@code upgrade-downgrade-fixtures/index-tables/hudi-v6-table-index-{type}.zip}.
 * Each fixture contains a MOR table with inline compaction, clustering and archival applied,
 * ending with 9 records (id2–id10; id1 was deleted in the final step).
 * See the fixtures README for the full timeline structure.
 *
 * <p>To regenerate fixtures:
 * <pre>
 *   cd hudi-spark-datasource/hudi-spark/src/test/resources/upgrade-downgrade-fixtures
 *   ./generate-fixtures.sh --version 6 --script-name generate-fixture-index.scala
 * </pre>
 */
class TestUpgradeFromV6IndexTypes extends HoodieClientTestBase {

  // Schema used by the index fixture tables (generate-fixture-index.scala)
  private static final StructType FIXTURE_SCHEMA = new StructType()
      .add("id", DataTypes.StringType, false)
      .add("name", DataTypes.StringType, false)
      .add("ts", DataTypes.LongType, false)
      .add("partition", DataTypes.StringType, false);

  // Expected record IDs present after fixture generation (id1 was deleted in Step 8)
  private static final List<String> EXPECTED_IDS =
      Arrays.asList("id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9", "id10");

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private static Stream<Arguments> indexTypeParameters() {
    return Stream.of(
        Arguments.of("bloom",         HoodieIndex.IndexType.BLOOM,         mapOf()),
        Arguments.of("global-bloom",  HoodieIndex.IndexType.GLOBAL_BLOOM,  mapOf()),
        Arguments.of("simple",        HoodieIndex.IndexType.SIMPLE,        mapOf()),
        Arguments.of("global-simple", HoodieIndex.IndexType.GLOBAL_SIMPLE, mapOf()),
        Arguments.of("bucket",        HoodieIndex.IndexType.BUCKET,
            mapOf("hoodie.bucket.index.num.buckets", "4")),
        Arguments.of("record-index",  HoodieIndex.IndexType.RECORD_INDEX,
            mapOf("hoodie.metadata.enable",                      "true",
                  "hoodie.metadata.record.index.enable",         "true",
                  "hoodie.metadata.index.column.stats.enable",   "false",
                  // Default is false for record index; set to true so the cross-partition
                  // upsert behaviour matches GLOBAL_BLOOM / GLOBAL_SIMPLE (record moves).
                  "hoodie.record.index.update.partition.path",   "true"))
    );
  }

  /**
   * Core index validation test for v6 tables read with the 1.1 binary (auto-upgrade disabled):
   * <ol>
   *   <li>Loads the v6 fixture and verifies the initial table state.</li>
   *   <li>Issues a single upsert that both updates an existing key (id2) and inserts a new key
   *       (id11). A working index must locate id2's existing file group and overwrite it,
   *       keeping the total count at 10. A broken index would insert a duplicate for id2
   *       and raise the count to 11.</li>
   *   <li>For global indexes, id2 is written to a different partition with
   *       {@code update.partition.path=true} enabled. A working global index deduplicates
   *       and moves the record (count stays at 10, partition updated). For local indexes the
   *       same partition is used. A broken index would insert a phantom duplicate (count 11).</li>
   *   <li>Confirms the table version remains at v6 (auto-upgrade is disabled).</li>
   * </ol>
   */
  @ParameterizedTest(name = "indexType={0}")
  @MethodSource("indexTypeParameters")
  void testUpgradePreservesIndexFunctionality(
      String indexName,
      HoodieIndex.IndexType indexType,
      Map<String, String> extraConfig) throws Exception {

    String fixtureName = "hudi-v6-table-index-" + indexName;
    HoodieTestUtils.extractZipToDirectory(
        "upgrade-downgrade-fixtures/index-tables/" + fixtureName + ".zip", tempDir, getClass());
    basePath = tempDir.resolve(fixtureName).toString();
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(basePath)
        .build();

    // ── Pre-write assertions ──────────────────────────────────────────────────
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion(),
        "fixture must be a v6 table");

    Dataset<Row> initialSnapshot = readTable();
    assertEquals(9, initialSnapshot.count(),
        "fixture ends with 9 records: id1 deleted in Step 8, id2-id10 remain");

    Set<String> initialIds = recordIdsIn(initialSnapshot);
    assertFalse(initialIds.contains("id1"), "id1 was deleted in the fixture");
    assertTrue(initialIds.containsAll(EXPECTED_IDS), "all expected IDs must be present");
    assertEquals("Bob_v2", nameOf("id2", initialSnapshot),
        "id2 was upserted to Bob_v2 in fixture Step 4");

    // ── Single upsert: update an existing key + insert a new key ─────────────
    // For global indexes, write id2 to a different partition to exercise cross-partition
    // lookup. update.partition.path is enabled for all global index types (true by default
    // for GLOBAL_BLOOM/GLOBAL_SIMPLE; explicitly set for RECORD_INDEX above), so the record
    // moves to the new partition. For local indexes the original partition is used.
    boolean isGlobal = indexType == HoodieIndex.IndexType.GLOBAL_BLOOM
        || indexType == HoodieIndex.IndexType.GLOBAL_SIMPLE
        || indexType == HoodieIndex.IndexType.RECORD_INDEX;
    String id2Partition = isGlobal ? "2023-01-02" : "2023-01-01";

    String tableName = "hudi_v6_table_index_" + indexName;
    Map<String, String> writeOptions = buildWriteOptions(tableName, indexType, extraConfig);

    sqlContext.sparkSession().createDataFrame(
            Arrays.asList(
                RowFactory.create("id2", "Bob_upgraded", 99999L, id2Partition),
                RowFactory.create("id11", "NewRecord", 100000L, "2023-01-01")),
            FIXTURE_SCHEMA)
        .write().format("hudi")
        .options(writeOptions)
        .option("hoodie.datasource.write.operation", "upsert")
        .mode(SaveMode.Append)
        .save(basePath);

    // ── Post-write content verification ──────────────────────────────────────
    Dataset<Row> postUpsertSnapshot = readTable();
    assertEquals(10, postUpsertSnapshot.count(),
        "upsert of existing id2 must not create a duplicate — index must be functional; id11 inserted");

    Set<String> postIds = recordIdsIn(postUpsertSnapshot);
    assertFalse(postIds.contains("id1"), "id1 must remain deleted");
    assertTrue(postIds.containsAll(EXPECTED_IDS), "id2-id10 must all be present");
    assertTrue(postIds.contains("id11"), "id11 must be inserted");

    // id2: name and partition must reflect what was written
    assertEquals("Bob_upgraded", nameOf("id2", postUpsertSnapshot), "id2 name must be updated");
    assertEquals(id2Partition, partitionOf("id2", postUpsertSnapshot),
        "global index with update.partition.path=true must move id2 to the new partition");

    // id11: new record content
    assertEquals("NewRecord", nameOf("id11", postUpsertSnapshot), "id11 name must match");
    assertEquals("2023-01-01", partitionOf("id11", postUpsertSnapshot), "id11 partition must match");

    // Spot-check a sample of untouched records to confirm no side-effects
    assertEquals("Charlie", nameOf("id3", postUpsertSnapshot));
    assertEquals("2023-01-01", partitionOf("id3", postUpsertSnapshot));
    assertEquals("David", nameOf("id4", postUpsertSnapshot));
    assertEquals("2023-01-02", partitionOf("id4", postUpsertSnapshot));
    assertEquals("Eve", nameOf("id5", postUpsertSnapshot));
    assertEquals("2023-01-02", partitionOf("id5", postUpsertSnapshot));

    // ── Version assertion ─────────────────────────────────────────────────────
    // auto.upgrade is disabled so the table version must remain at v6.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion(),
        "table version must remain at v6 when auto-upgrade is disabled");
  }

  // ── Helpers ───────────────────────────────────────────────────────────────────

  /**
   * Base write options shared by all index types. Non-RECORD_INDEX types disable
   * metadata to avoid the v6 MDT bootstrap-guard issue (the v6 fixture's metadata table,
   * if present, only has the {@code files} partition; 1.1 defaults would attempt to
   * bootstrap additional partitions and be rejected). RECORD_INDEX overrides
   * {@code hoodie.metadata.enable} back to {@code true} via {@code extraConfig}.
   */
  private static Map<String, String> buildWriteOptions(
      String tableName,
      HoodieIndex.IndexType indexType,
      Map<String, String> extraConfig) {
    Map<String, String> options = new HashMap<>();
    options.put("hoodie.datasource.write.recordkey.field", "id");
    options.put("hoodie.datasource.write.partitionpath.field", "partition");
    options.put("hoodie.datasource.write.precombine.field", "ts");
    options.put("hoodie.table.name", tableName);
    options.put("hoodie.datasource.write.table.type", "MERGE_ON_READ");
    options.put("hoodie.index.type", indexType.name());
    options.put("hoodie.write.auto.upgrade", "false");
    options.put("hoodie.metadata.enable", "false");
    options.put("hoodie.metadata.index.column.stats.enable", "false");
    options.putAll(extraConfig);
    return options;
  }

  private Dataset<Row> readTable() {
    return sqlContext.read().format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(basePath);
  }

  private static Set<String> recordIdsIn(Dataset<Row> snapshot) {
    return snapshot.select("id")
        .distinct()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }

  private static String nameOf(String id, Dataset<Row> snapshot) {
    return snapshot.filter("id = '" + id + "'")
        .select("name")
        .collectAsList()
        .get(0)
        .getString(0);
  }

  private static String partitionOf(String id, Dataset<Row> snapshot) {
    return snapshot.filter("id = '" + id + "'")
        .select("partition")
        .collectAsList()
        .get(0)
        .getString(0);
  }

  /** Varargs helper for building String maps in Java 8 (no Map.of). */
  private static Map<String, String> mapOf(String... kv) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }
}
