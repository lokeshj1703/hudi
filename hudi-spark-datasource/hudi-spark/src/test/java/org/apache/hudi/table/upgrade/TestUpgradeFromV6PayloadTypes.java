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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates that the 1.1 binary correctly reads and writes Hudi 0.14 (table version 6) MOR
 * tables created with different payload classes, and that the payload merge semantics continue
 * to function correctly — specifically that the MOR snapshot merge applies the right record
 * version and that upserts respect each payload's ordering contract.
 *
 * <p>Fixtures: {@code upgrade-downgrade-fixtures/payload-tables/hudi-v6-table-payload-{type}.zip}.
 * Each fixture is a non-partitioned MOR table produced by {@code generate-fixture-payload.scala},
 * ending with lsn=3 and lsn=5 physically deleted, and lsn=6/lsn=7 freshly inserted.
 * See the fixtures README for the full timeline structure.
 *
 * <p>To regenerate fixtures:
 * <pre>
 *   cd hudi-spark-datasource/hudi-spark/src/test/resources/upgrade-downgrade-fixtures
 *   ./generate-fixtures.sh --version 6 --script-name generate-fixture-payload.scala
 * </pre>
 */
class TestUpgradeFromV6PayloadTypes extends HoodieClientTestBase {

  // Schema matches generate-fixture-payload.scala:
  //   ts, _event_lsn, rider, driver, fare, Op, _event_seq,
  //   _event_bin_file (FLATTENED_FILE_COL_NAME),
  //   _event_pos      (FLATTENED_POS_COL_NAME),
  //   _change_operation_type (FLATTENED_OP_COL_NAME)
  private static final StructType PAYLOAD_SCHEMA = new StructType()
      .add("ts",                     DataTypes.IntegerType, false)
      .add("_event_lsn",             DataTypes.LongType,    false)
      .add("rider",                  DataTypes.StringType,  false)
      .add("driver",                 DataTypes.StringType,  true)   // nullable: Partial omits this field
      .add("fare",                   DataTypes.DoubleType,  false)
      .add("Op",                     DataTypes.StringType,  false)
      .add("_event_seq",             DataTypes.StringType,  false)
      .add("_event_bin_file",        DataTypes.IntegerType, false)
      .add("_event_pos",             DataTypes.IntegerType, false)
      .add("_change_operation_type", DataTypes.StringType,  false);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private static Stream<Arguments> payloadTypeParameters() {
    return Stream.of(
        // Default: ordering by ts; Op="D" acts as a delete marker (configured via extraConfig)
        Arguments.of("default",
            "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
            "ts",
            mapOf("hoodie.payload.delete.field", "Op",
                  "hoodie.payload.delete.marker", "D"),
            true),
        // Overwrite: preCombine always picks the incoming record; no ordering check
        Arguments.of("overwrite",
            "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
            "ts",
            mapOf(),
            false),
        // Partial: ordering by ts; only non-null fields from the incoming record are merged.
        // A null driver in the high-ordering row leaves the stored driver intact.
        Arguments.of("partial",
            "org.apache.hudi.common.model.PartialUpdateAvroPayload",
            "ts",
            mapOf(),
            true),
        // Postgres Debezium: ordering by _event_lsn, which is also the record key in this fixture.
        // In real Postgres CDC usage, the record key is a business key and _event_lsn is a
        // separate change-sequence field. Because key == ordering here, every upsert to an
        // existing record has insertLSN == currentLSN. shouldPickCurrentRecord returns false
        // (keeps current only when insertLSN < currentLSN), so equal-LSN always applies the
        // incoming record. The "lower LSN is rejected" branch cannot be exercised without a
        // fixture that separates the key from _event_lsn.
        Arguments.of("postgres",
            "org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload",
            "_event_lsn",
            mapOf(),
            false),
        // MySQL Debezium: ordering by _event_seq as "file.pos" string (e.g. "10.1");
        // higher file number or position wins
        Arguments.of("mysql",
            "org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload",
            "_event_seq",
            mapOf(),
            true),
        // AWS DMS: Op="D" triggers delete; preCombine delegates to OverwriteWithLatest
        Arguments.of("awsdms",
            "org.apache.hudi.common.model.AWSDmsAvroPayload",
            "ts",
            mapOf(),
            false),
        // EventTime: ordering by ts; no delete marker support
        Arguments.of("eventtime",
            "org.apache.hudi.common.model.EventTimeAvroPayload",
            "ts",
            mapOf(),
            true),
        // OverwriteNonDefaults: like Overwrite but preserves existing non-default field values
        Arguments.of("overwritenondefaults",
            "org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload",
            "ts",
            mapOf(),
            false)
    );
  }

  /**
   * Core payload merge validation test for v6 tables read with the 1.1 binary (auto-upgrade disabled):
   * <ol>
   *   <li>Loads the v6 fixture and verifies the initial table state — lsn=3/5 are physically
   *       deleted; lsn=6/7 are freshly inserted with known rider values.</li>
   *   <li>Issues a single upsert that both updates an existing key (lsn=4) and inserts a new
   *       key (lsn=100). All ordering-related fields (ts, _event_seq, _event_bin_file) are set
   *       to extreme-high values so the update wins under every payload's ordering contract.</li>
   *   <li>Verifies the post-upsert snapshot: count increases by 1 (lsn=100 added, lsn=4 updated
   *       in place), lsn=4 reflects the new rider, untouched records remain unchanged, and
   *       previously deleted keys stay deleted.</li>
   *   <li>Confirms the table version remains at v6 (auto-upgrade is disabled).</li>
   * </ol>
   */
  @ParameterizedTest(name = "payloadType={0}")
  @MethodSource("payloadTypeParameters")
  void testPayloadMergeSemanticsFunctionality(
      String payloadName,
      String payloadClass,
      String precombineField,
      Map<String, String> extraConfig,
      boolean supportsOrdering) throws Exception {

    String fixtureName = "hudi-v6-table-payload-" + payloadName;
    HoodieTestUtils.extractZipToDirectory(
        "upgrade-downgrade-fixtures/payload-tables/" + fixtureName + ".zip", tempDir, getClass());
    basePath = tempDir.resolve(fixtureName).toString();
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance())
        .setBasePath(basePath)
        .build();

    // ── Pre-write assertions ──────────────────────────────────────────────────
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion(),
        "fixture must be a v6 table");

    Dataset<Row> initialSnapshot = readTable();
    long initialCount = initialSnapshot.count();

    // lsn=3 and lsn=5 are physically deleted in fixture step 5 (independent of payload type)
    assertFalse(lsnExists(3L, initialSnapshot), "lsn=3 must be deleted in fixture (step 5)");
    assertFalse(lsnExists(5L, initialSnapshot), "lsn=5 must be deleted in fixture (step 5)");

    // lsn=6 and lsn=7 are freshly inserted in fixture step 6 (independent of payload type)
    assertTrue(lsnExists(6L, initialSnapshot), "lsn=6 must be present from fixture step 6");
    assertEquals("rider-G", riderOf(6L, initialSnapshot),
        "lsn=6 must have rider-G from fixture step 6");
    assertTrue(lsnExists(7L, initialSnapshot), "lsn=7 must be present from fixture step 6");
    assertEquals("rider-H", riderOf(7L, initialSnapshot),
        "lsn=7 must have rider-H from fixture step 6");

    // ── Single upsert: update lsn=4 + insert lsn=100 ─────────────────────────
    // All ordering fields are set to extreme-high values so the update wins under every
    // payload's ordering contract:
    //   - ts-based payloads (Default, Partial, EventTime, etc.): ts=9999 >> fixture max ts=13
    //   - MySQL Debezium (_event_seq "file.pos"): "9999.1" → file=9999 >> fixture max file=13
    //   - Postgres Debezium: _event_lsn is both the record key and the ordering field in this
    //     fixture. Upserting key=4 means insertLSN=4 == currentLSN=4; shouldPickCurrentRecord
    //     returns false (keeps current only when insertLSN < currentLSN), so the equal-LSN
    //     case always applies. A strictly-higher insertLSN is impossible without creating a
    //     new record (different key), because key == _event_lsn in this fixture.
    //   - OverwriteWithLatest / AWSDms: preCombine always picks the incoming record
    String tableName = "hudi_v6_table_payload_" + payloadName;
    Map<String, String> writeOptions = buildWriteOptions(tableName, payloadClass, precombineField, extraConfig);

    // For PartialUpdateAvroPayload, omit driver (null) in the high-ordering row so the
    // stored driver is preserved via partial-merge semantics.
    boolean isPartial = payloadClass.contains("PartialUpdate");
    List<Row> upsertRows = new ArrayList<>(Arrays.asList(
        // High ordering: wins for every payload type
        RowFactory.create(9999, 4L, "rider-NEW", isPartial ? null : "driver-NEW", 99.99, "u", "9999.1", 9999, 1, "u"),
        // Insert lsn=100: brand-new key
        RowFactory.create(9999, 100L, "rider-NEW-100", "driver-NEW-100", 88.88, "i", "9999.1", 9999, 1, "i")));
    if (supportsOrdering) {
      // Stale update for lsn=2: ordering value (ts=1 / "_event_seq=1.1") is lower than the
      // stored record (ts=11 / "_event_seq=11.1"). At MOR read time the payload's ordering
      // check must reject this log record and keep the stored record (rider-Y, ts=11).
      upsertRows.add(RowFactory.create(1, 2L, "rider-STALE", "driver-STALE", 0.01, "u", "1.1", 1, 1, "u"));
    }
    sqlContext.sparkSession().createDataFrame(upsertRows, PAYLOAD_SCHEMA)
        .write().format("hudi")
        .options(writeOptions)
        .option("hoodie.datasource.write.operation", "upsert")
        .mode(SaveMode.Append)
        .save(basePath);

    // ── Post-write content verification ──────────────────────────────────────
    Dataset<Row> postUpsertSnapshot = readTable();

    // lsn=4 updated in place; lsn=100 added as new record; lsn=2 stale update rejected by ordering
    assertEquals(initialCount + 1, postUpsertSnapshot.count(),
        "lsn=4 updated in place, lsn=2 stale update rejected — count increases by 1 for lsn=100 only");

    // lsn=4 must reflect the updated rider
    assertTrue(lsnExists(4L, postUpsertSnapshot), "lsn=4 must still exist after upsert");
    assertEquals("rider-NEW", riderOf(4L, postUpsertSnapshot),
        "lsn=4 rider must be updated by the high-ordering row; stale row (ts=1) must be rejected");
    if (isPartial) {
      // driver was null in the high-ordering row → PartialUpdateAvroPayload (IGNORE_DEFAULTS)
      // preserves the stored driver value. lsn=4 stored state after MOR merge:
      //   base parquet: ts=10, driver-D  (from initial bulk_insert)
      //   log file:     ts=9,  driver-DD (from secondUpdateData — LOWER ordering than base)
      // At read time the ts=9 log record is rejected because ts=9 < ts=10; the stored driver
      // remains driver-D. Our test upsert (ts=9999, driver=null) wins on ordering, but its
      // null driver field is skipped by IGNORE_DEFAULTS, so driver-D is preserved.
      assertEquals("driver-D", driverOf(4L, postUpsertSnapshot),
          "driver must be preserved from stored record when omitted in partial update");
    }

    // lsn=100: new insert
    assertTrue(lsnExists(100L, postUpsertSnapshot), "lsn=100 must be inserted");
    assertEquals("rider-NEW-100", riderOf(100L, postUpsertSnapshot),
        "lsn=100 rider must match the inserted value");

    // lsn=2 must not be overwritten by the stale row (ts=1 < stored ts=11 after fixture step 2)
    if (supportsOrdering) {
      assertEquals("rider-Y", riderOf(2L, postUpsertSnapshot),
          "lsn=2 must retain rider-Y; stale row with ts=1 must be rejected by ordering");
    }

    // lsn=6 and lsn=7 must remain unchanged
    assertEquals("rider-G", riderOf(6L, postUpsertSnapshot), "lsn=6 must be unchanged");
    assertEquals("rider-H", riderOf(7L, postUpsertSnapshot), "lsn=7 must be unchanged");

    // lsn=3 and lsn=5 must still be absent
    assertFalse(lsnExists(3L, postUpsertSnapshot), "lsn=3 must remain deleted");
    assertFalse(lsnExists(5L, postUpsertSnapshot), "lsn=5 must remain deleted");

    // ── Version assertion ─────────────────────────────────────────────────────
    // auto.upgrade is disabled so the table version must remain at v6.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion(),
        "table version must remain at v6 when auto-upgrade is disabled");
  }

  // ── Helpers ───────────────────────────────────────────────────────────────────

  /**
   * Base write options shared by all payload types. Auto-upgrade is disabled so the table
   * version stays at v6 and HoodieTableMetaClient.reload() does not reject the unchanged
   * timeline layout version.
   */
  private static Map<String, String> buildWriteOptions(
      String tableName,
      String payloadClass,
      String precombineField,
      Map<String, String> extraConfig) {
    Map<String, String> options = new HashMap<>();
    options.put("hoodie.datasource.write.recordkey.field", "_event_lsn");
    options.put("hoodie.datasource.write.partitionpath.field", "");
    options.put("hoodie.datasource.write.precombine.field", precombineField);
    options.put("hoodie.table.name", tableName);
    options.put("hoodie.datasource.write.table.type", "MERGE_ON_READ");
    options.put("hoodie.datasource.write.payload.class", payloadClass);
    options.put("hoodie.write.auto.upgrade", "false");
    options.put("hoodie.metadata.enable", "false");
    options.putAll(extraConfig);
    return options;
  }

  private Dataset<Row> readTable() {
    return sqlContext.read().format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(basePath);
  }

  private static boolean lsnExists(long lsn, Dataset<Row> snapshot) {
    return snapshot.filter("`_event_lsn` = " + lsn).count() > 0;
  }

  private static String riderOf(long lsn, Dataset<Row> snapshot) {
    return snapshot.filter("`_event_lsn` = " + lsn)
        .select("rider")
        .collectAsList()
        .get(0)
        .getString(0);
  }

  private static String driverOf(long lsn, Dataset<Row> snapshot) {
    return snapshot.filter("`_event_lsn` = " + lsn)
        .select("driver")
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
