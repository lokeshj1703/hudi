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

package org.apache.hudi.cli;

import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.index.SparkMetadataTableRecordIndex;
import org.apache.hudi.index.simple.HoodieGlobalSimpleIndex;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import scala.reflect.ClassTag;

/**
 * Archive Utils.
 */
public final class ArchiveExecutorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ArchiveExecutorUtils.class);

  private ArchiveExecutorUtils() {
  }

  public static class HoodieMetadataValidationContext implements AutoCloseable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataValidationContext.class);

    private final HoodieTableMetaClient metaClient;
    private final HoodieTableFileSystemView fileSystemView;
    private final HoodieTableMetadata tableMetadata;
    private final boolean enableMetadataTable;
    private List<String> allColumnNameList;

    private HoodieEngineContext engineContext;

    public HoodieMetadataValidationContext(
        HoodieEngineContext engineContext, HoodieTableMetaClient metaClient,
        boolean enableMetadataTable) {
      this.metaClient = metaClient;
      this.enableMetadataTable = enableMetadataTable;
      HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
          .enable(enableMetadataTable)
          .build();
      this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext,
          metaClient, metadataConfig);
      this.tableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, metaClient.getBasePath());
      if (metaClient.getCommitsTimeline().filterCompletedInstants().countInstants() > 0) {
        this.allColumnNameList = getAllColumnNames();
      }
      this.engineContext = engineContext;
    }

    public HoodieTableMetaClient getMetaClient() {
      return metaClient;
    }

    private List<String> getAllColumnNames() {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
      try {
        return schemaResolver.getTableAvroSchema().getFields().stream()
            .map(Schema.Field::name).collect(Collectors.toList());
      } catch (Exception e) {
        throw new HoodieException("Failed to get all column names for " + metaClient.getBasePath());
      }
    }

    @Override
    public void close() throws Exception {
      tableMetadata.close();
      fileSystemView.close();
    }
  }

  public static void validateRecordIndex(HoodieMetadataValidationContext metadataTableBasedContext) {
    SparkSession spark = SparkSession.builder().getOrCreate();
    Dataset<Row> df = spark.read().format("hudi").load(metadataTableBasedContext.metaClient.getBasePathV2().toString());
    Dataset<String> recordKeys = df.select("_hoodie_record_key").map((MapFunction<Row, String>) value -> value.getString(0), Encoders.STRING());
    HoodieBackedTableMetadata tableMetadata = ((HoodieBackedTableMetadata)metadataTableBasedContext.tableMetadata);
    int numFileGroups = tableMetadata.getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX);
    JavaRDD<String> partitionedKeyRDD = JavaRDD.fromRDD(recordKeys.rdd(), ClassTag.apply(String.class)).keyBy(k -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(k, numFileGroups))
        .partitionBy(new SparkMetadataTableRecordIndex.PartitionIdPassthrough(numFileGroups))
        .map(t -> t._2);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withProps(metadataTableBasedContext.metaClient.getTableConfig().getProps())
        .withPath(metadataTableBasedContext.metaClient.getBasePathV2().toString())
        .build();
    HoodieTable hoodieTable = HoodieSparkTable.create(config, metadataTableBasedContext.engineContext, metadataTableBasedContext.metaClient);
    HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations =
        HoodieJavaPairRDD.of(partitionedKeyRDD.mapPartitionsToPair(new SparkMetadataTableRecordIndex.RecordIndexFileGroupLookupFunction(hoodieTable)));

    HoodieWriteConfig globalSimpleConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps()).build();
    globalSimpleConfig.setValue(HoodieIndexConfig.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_SIMPLE.name());
    HoodieGlobalSimpleIndex globalSimpleIndex = (HoodieGlobalSimpleIndex) SparkHoodieIndexFactory.createIndex(globalSimpleConfig);
    List<Pair<String, HoodieBaseFile>> latestBaseFiles = globalSimpleIndex.getAllBaseFilesInTable(hoodieTable.getContext(), hoodieTable);
    HoodiePairData<String, HoodieRecordGlobalLocation> allKeysAndLocations =
        globalSimpleIndex.fetchRecordGlobalLocations(hoodieTable.getContext(), hoodieTable, config.getGlobalSimpleIndexParallelism(), latestBaseFiles);
    AtomicInteger nonExistingRLIKeys = new AtomicInteger();
    AtomicInteger nonMatchingKeys = new AtomicInteger();
    long matchingKeys = allKeysAndLocations.leftOuterJoin(keyAndExistingLocations).values().filter(p -> {
      if (!p.getRight().isPresent()) {
        nonExistingRLIKeys.getAndIncrement();
        LOG.error("RLI location does not exist for GSI location {}", p.getLeft());
        return false;
      } else if (!matchRecordGlobalLocation(p.getLeft(), p.getRight().get())) {
        LOG.error("Location does not match GSI {} RLI {}", p.getLeft(), p.getRight().get());
        nonMatchingKeys.getAndIncrement();
        return false;
      }
      return true;
    }).count();
    if (nonExistingRLIKeys.get() > 0 || nonMatchingKeys.get() > 0) {
      LOG.error("Validation failed nonExistingRLIKeys {} nonMatchingKeys {} matching {}", nonExistingRLIKeys, nonMatchingKeys, matchingKeys);
      throw new RuntimeException("Asdsakjdnbsakj");
    }
    long gsiCount = allKeysAndLocations.count();
    long rliCount = keyAndExistingLocations.count();
    if (gsiCount != keyAndExistingLocations.count()) {
      LOG.error("Count mismatch between RLI {} and GSI {}", rliCount, gsiCount);
    }
  }

  private static boolean matchRecordGlobalLocation(HoodieRecordGlobalLocation left, HoodieRecordGlobalLocation right) {
    if (!left.getPartitionPath().equals(right.getPartitionPath())) {
      return false;
    } else {
      return left.getFileId().equals(right.getFileId());
    }
  }

  public static int archive(JavaSparkContext jsc,
       int minCommits,
       int maxCommits,
       int commitsRetained,
       boolean enableMetadata,
       String basePath) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(minCommits, maxCommits).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(commitsRetained).build())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata).build())
        .build();
    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    HoodieSparkTable<HoodieAvroPayload> table = HoodieSparkTable.create(config, context);
    try {
      HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(config, table);
      archiver.archiveIfRequired(context, true);
    } catch (IOException ioe) {
      LOG.error("Failed to archive with IOException: " + ioe);
      return -1;
    }
    return 0;
  }
}
