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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieDeltaStreamerSchemaEvolutionQuick extends TestHoodieDeltaStreamerSchemaEvolutionBase {

  protected static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    //only testing row-writer enabled for now
    for (Boolean rowWriterEnable : new Boolean[] {true}) {
      for (Boolean nullForDeletedCols : new Boolean[] {false, true}) {
        for (Boolean useKafkaSource : new Boolean[] {false, true}) {
          for (Boolean addFilegroups : new Boolean[] {false, true}) {
            for (Boolean multiLogFiles : new Boolean[] {false, true}) {
              for (Boolean shouldCluster : new Boolean[] {false, true}) {
                for (String tableType : new String[] {"COPY_ON_WRITE", "MERGE_ON_READ"}) {
                  if (!multiLogFiles || tableType.equals("MERGE_ON_READ")) {
                    b.add(Arguments.of(tableType, shouldCluster, false, rowWriterEnable, addFilegroups, multiLogFiles, useKafkaSource, nullForDeletedCols));
                  }
                }
              }
              b.add(Arguments.of("MERGE_ON_READ", false, true, rowWriterEnable, addFilegroups, multiLogFiles, useKafkaSource, nullForDeletedCols));
            }
          }
        }
      }
    }
    return b.build();
  }

  protected static Stream<Arguments> testIncompatibleEvolution() {
    Stream.Builder<Arguments> b = Stream.builder();
    for (Boolean rowWriterEnable : new Boolean[] {true}) {
      for (Boolean nullForDeletedCols : new Boolean[] {false, true}) {
        for (Boolean useKafkaSource : new Boolean[] {false, true}) {
          for (String tableType : new String[] {"COPY_ON_WRITE", "MERGE_ON_READ"}) {
            b.add(Arguments.of(tableType, rowWriterEnable, useKafkaSource, nullForDeletedCols));
          }
        }
      }
    }
    return b.build();
  }

  protected static Stream<Arguments> testParamsWithSchemaTransformer() {
    Stream.Builder<Arguments> b = Stream.builder();
    for (Boolean useTransformer : new Boolean[] {false, true}) {
      for (Boolean setSchema : new Boolean[] {false, true}) {
        for (Boolean rowWriterEnable : new Boolean[] {true}) {
          for (Boolean nullForDeletedCols : new Boolean[] {false, true}) {
            for (Boolean useKafkaSource : new Boolean[] {false, true}) {
              for (String tableType : new String[] {"COPY_ON_WRITE", "MERGE_ON_READ"}) {
                b.add(Arguments.of(tableType, rowWriterEnable, useKafkaSource, nullForDeletedCols, useTransformer, setSchema));
              }
            }
          }
        }
      }
    }
    return b.build();
  }

  /**
   * Main testing logic for non-type promotion tests
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testBase(String tableType,
                          Boolean shouldCluster,
                          Boolean shouldCompact,
                          Boolean rowWriterEnable,
                          Boolean addFilegroups,
                          Boolean multiLogFiles,
                          Boolean useKafkaSource,
                          Boolean allowNullForDeletedCols) throws Exception {
    this.tableType = tableType;
    this.shouldCluster = shouldCluster;
    this.shouldCompact = shouldCompact;
    this.rowWriterEnable = rowWriterEnable;
    this.addFilegroups = addFilegroups;
    this.multiLogFiles = multiLogFiles;
    this.useKafkaSource = useKafkaSource;
    if (useKafkaSource) {
      this.useSchemaProvider = true;
    }
    this.useTransformer = true;
    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableBasePath = basePath + "test_parquet_table" + testNum;
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(allowNullForDeletedCols), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (multiLogFiles) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    //make other filegroups
    if (addFilegroups) {
      datapath = String.class.getResource("/data/schema-evolution/newFileGroupsTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      numRecords += 3;
      numFiles += 3;
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, isCow);
    }

    //write updates
    datapath = String.class.getResource("/data/schema-evolution/endTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    //do casting
    Column col = df.col("tip_history");
    df = df.withColumn("tip_history", col.cast(DataTypes.createArrayType(DataTypes.LongType)));
    col = df.col("fare");
    df = df.withColumn("fare", col.cast(DataTypes.createStructType(new StructField[]{
        new StructField("amount", DataTypes.StringType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty()),
        new StructField("zextra_col_nested", DataTypes.StringType, true, Metadata.empty())
    })));
    col = df.col("begin_lat");
    df = df.withColumn("begin_lat", col.cast(DataTypes.DoubleType));
    col = df.col("end_lat");
    df = df.withColumn("end_lat", col.cast(DataTypes.StringType));
    col = df.col("distance_in_meters");
    df = df.withColumn("distance_in_meters", col.cast(DataTypes.FloatType));
    col = df.col("seconds_since_epoch");
    df = df.withColumn("seconds_since_epoch", col.cast(DataTypes.StringType));

    try {
      addData(df, false);
      deltaStreamer.sync();
    } catch (SchemaCompatibilityException e) {
      assertTrue(e.getMessage().contains("Incoming batch schema is not compatible with the table's one"));
      assertFalse(allowNullForDeletedCols);
      return;
    }

    if (shouldCluster) {
      //everything combines into 1 file per partition
      assertBaseFileOnlyNumber(3);
    } else if (shouldCompact || isCow) {
      assertBaseFileOnlyNumber(numFiles);
    } else {
      numFiles += 2;
      assertFileNumber(numFiles, false);
    }
    assertRecordCount(numRecords);

    df = sparkSession.read().format("hudi").load(tableBasePath);
    df.show(100,false);
    df.cache();
    assertDataType(df, "tip_history", DataTypes.createArrayType(DataTypes.LongType));
    assertDataType(df, "fare", DataTypes.createStructType(new StructField[]{
        new StructField("amount", DataTypes.StringType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty()),
        new StructField("extra_col_struct", DataTypes.LongType, true, Metadata.empty()),
        new StructField("zextra_col_nested", DataTypes.StringType, true, Metadata.empty())
    }));
    assertDataType(df, "begin_lat", DataTypes.DoubleType);
    assertDataType(df, "end_lat", DataTypes.StringType);
    assertDataType(df, "distance_in_meters", DataTypes.FloatType);
    assertDataType(df, "seconds_since_epoch", DataTypes.StringType);
    assertCondition(df, "zextra_col = 'yes'", 2);
    assertCondition(df, "_extra_col = 'yes'", 2);
    assertCondition(df, "fare.zextra_col_nested = 'yes'", 2);
    assertCondition(df, "size(zcomplex_array) > 0", 2);
    assertCondition(df, "extra_col_regular is NULL", 2);
    assertCondition(df, "fare.extra_col_struct is NULL", 2);
  }


  /**
   * Main testing logic for non-type promotion tests
   */
  @ParameterizedTest
  @MethodSource("testIncompatibleEvolution")
  public void testIncompatibleEvolution(String tableType,
                       Boolean rowWriterEnable,
                       Boolean useKafkaSource,
                       Boolean allowNullForDeletedCols) throws Exception {
    this.tableType = tableType;
    this.rowWriterEnable = rowWriterEnable;
    this.useKafkaSource = useKafkaSource;
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.useTransformer = true;
    if (useKafkaSource) {
      this.useSchemaProvider = true;
    }

    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableBasePath = basePath + "test_parquet_table" + testNum;
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(allowNullForDeletedCols), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (tableType.equals("MERGE_ON_READ")) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    //write updates
    datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    df.drop("rider");

    try {
      addData(df, false);
      deltaStreamer.sync();
    } catch (SchemaCompatibilityException e) {
      assertTrue(e.getMessage().contains("Incoming batch schema is not compatible with the table's one"));
      assertFalse(allowNullForDeletedCols);
    }

    assertRecordCount(numRecords);

    datapath = String.class.getResource("/data/schema-evolution/IncompatibleTestEverything_reordered_column.json").getPath();
    df = sparkSession.read().json(datapath);
    df = df.drop("rider").withColumn("rider", functions.lit("rider-003"));

    try {
      addData(df, false);
      deltaStreamer.sync();
      assertTrue(allowNullForDeletedCols);
    } catch (SchemaCompatibilityException e) {
      assertTrue(e.getMessage().contains("Incoming batch schema is not compatible with the table's one"));
      assertFalse(allowNullForDeletedCols);
    }
  }

  @ParameterizedTest
  @MethodSource("testParamsWithSchemaTransformer")
  public void testIngestionWithOlderSchema(String tableType,
                                           Boolean rowWriterEnable,
                                           Boolean useKafkaSource,
                                           Boolean allowNullForDeletedCols,
                                           Boolean useTransformer,
                                           Boolean setSchema) throws Exception {
    this.tableType = tableType;
    this.rowWriterEnable = rowWriterEnable;
    this.useKafkaSource = useKafkaSource;
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.useTransformer = useTransformer;
    if (useKafkaSource) {
      this.useSchemaProvider = true;
    }

    boolean isCow = tableType.equals("COPY_ON_WRITE");
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;
    tableBasePath = basePath + "test_parquet_table" + testNum;
    String[] transformerClassNames = useTransformer ? new String[] {TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()}
        : new String[0];
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(transformerClassNames, allowNullForDeletedCols), jsc);

    //first write
    String datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    Dataset<Row> df = sparkSession.read().json(datapath);
    addData(df, true);
    deltaStreamer.sync();
    int numRecords = 6;
    int numFiles = 3;
    assertRecordCount(numRecords);
    assertFileNumber(numFiles, isCow);

    //add extra log files
    if (tableType.equals("MERGE_ON_READ")) {
      datapath = String.class.getResource("/data/schema-evolution/extraLogFilesTestEverything.json").getPath();
      df = sparkSession.read().json(datapath);
      addData(df, false);
      deltaStreamer.sync();
      //this write contains updates for the 6 records from the first write, so
      //although we have 2 files for each filegroup, we only see the log files
      //represented in the read. So that is why numFiles is 3, not 6
      assertRecordCount(numRecords);
      assertFileNumber(numFiles, false);
    }

    if (setSchema) {
      TestSchemaProvider.setTargetSchema(TestSchemaProvider.sourceSchema);
    }

    // drop column
    datapath = String.class.getResource("/data/schema-evolution/startTestEverything.json").getPath();
    df = sparkSession.read().json(datapath);
    df = df.drop("rider");
    try {
      addData(df, false);
      deltaStreamer.sync();
      assertTrue(allowNullForDeletedCols || (!allowNullForDeletedCols && setSchema));
    } catch (SchemaCompatibilityException e) {
      assertTrue(e.getMessage().contains("Incoming batch schema is not compatible with the table's one"));
      assertFalse(allowNullForDeletedCols);
    }

    if (setSchema) {
      TestSchemaProvider.setTargetSchema(TestSchemaProvider.sourceSchema);
    }

    // type promotion
    Column col = df.col("distance_in_meters");
    df = df.withColumn("distance_in_meters", col.cast(DataTypes.DoubleType));
    try {
      addData(df, false);
      deltaStreamer.sync();
      assertTrue(allowNullForDeletedCols && !setSchema);
    } catch (Exception e) {
      assertTrue(!allowNullForDeletedCols || (allowNullForDeletedCols && setSchema));
      if (!useKafkaSource) {
        e.printStackTrace();
        assertTrue(containsErrorMessage(e, "Incoming batch schema is not compatible with the table's one",
                "org.apache.spark.sql.catalyst.expressions.MutableDouble cannot be cast to org.apache.spark.sql.catalyst.expressions.MutableLong"),
            e.getMessage());
      } else {
        e.printStackTrace();
        assertTrue(containsErrorMessage(e, "Incoming batch schema is not compatible with the table's one",
                "cannot support rewrite value for schema type: \"long\" since the old schema type is: \"double\""),
            e.getMessage());
      }
    }

    if (setSchema) {
      TestSchemaProvider.setTargetSchema(TestSchemaProvider.sourceSchema);
    }

    // type demotion
    col = df.col("current_ts");
    df = df.withColumn("current_ts", col.cast(DataTypes.IntegerType));
    try {
      addData(df, false);
      deltaStreamer.sync();
      assertTrue(allowNullForDeletedCols && !setSchema);
    } catch (Exception e) {
      assertTrue(!allowNullForDeletedCols || (allowNullForDeletedCols && setSchema));
      if (!useKafkaSource) {
        e.printStackTrace();
        assertTrue(containsErrorMessage(e, "Incoming batch schema is not compatible with the table's one",
                "org.apache.spark.sql.catalyst.expressions.MutableInt cannot be cast to org.apache.spark.sql.catalyst.expressions.MutableLong"),
            e.getMessage());
      } else {
        assertTrue(containsErrorMessage(e, "Incoming batch schema is not compatible with the table's one",
                "cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""),
            e.getMessage());
      }
    }

    TestSchemaProvider.resetTargetSchema();
  }

  private boolean containsErrorMessage(Throwable e, String... messages) {
    while (e != null) {
      for (String msg : messages) {
        if (e.getMessage().contains(msg)) {
          return true;
        }
      }
      e = e.getCause();
    }

    return false;
  }

  protected void assertDataType(Dataset<Row> df, String colName, DataType expectedType) {
    assertEquals(expectedType, df.select(colName).schema().fields()[0].dataType());
  }

  protected void assertCondition(Dataset<Row> df, String condition, int count) {
    assertEquals(count, df.filter(condition).count());
  }

}
