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

package org.apache.hudi.io;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class HoodieMergedReadHandle<T, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  protected final Schema readerSchema;
  protected final Schema baseFileReaderSchema;
  private final Option<FileSlice> fileSliceOpt;

  public HoodieMergedReadHandle(HoodieWriteConfig config,
                                Option<String> instantTime,
                                HoodieTable<T, I, K, O> hoodieTable,
                                Pair<String, String> partitionPathFileIDPair) {
    this(config, instantTime, hoodieTable, partitionPathFileIDPair, Option.empty());
  }

  public HoodieMergedReadHandle(HoodieWriteConfig config,
                                Option<String> instantTime,
                                HoodieTable<T, I, K, O> hoodieTable,
                                Pair<String, String> partitionPathFileIDPair,
                                Option<FileSlice> fileSliceOption) {
    super(config, instantTime, hoodieTable, partitionPathFileIDPair);
    readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    // config.getSchema is not canonicalized, while config.getWriteSchema is canonicalized. So, we have to use the canonicalized schema to read the existing data.
    baseFileReaderSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getWriteSchema()), config.allowOperationMetadataField());
    fileSliceOpt = fileSliceOption.isPresent() ? fileSliceOption : getLatestFileSlice();
  }

  public Iterator<HoodieRecord<T>> getMergedRecordsItr() {
    if (!fileSliceOpt.isPresent()) {
      return Collections.emptyIterator();
    }
    checkState(nonEmpty(instantTime), String.format("Expected a valid instant time but got `%s`", instantTime));
    final FileSlice fileSlice = fileSliceOpt.get();
    String baseFileInstantTime = fileSlice.getBaseFile().get().getCommitTime();
    final HoodieRecordLocation currentLocation = new HoodieRecordLocation(baseFileInstantTime, fileSlice.getFileId());
    Option<HoodieFileReader> baseFileReader = Option.empty();
    HoodieMergedLogRecordScanner logRecordScanner = null;
    try {
      baseFileReader = getBaseFileReader(fileSlice);
      logRecordScanner = getLogRecordScanner(fileSlice);
      return new MergedRecordsIterator(baseFileReader, logRecordScanner, currentLocation);
    } catch (IOException e) {
      if (baseFileReader.isPresent()) {
        baseFileReader.get().close();
      }
      if (logRecordScanner != null) {
        logRecordScanner.close();
      }
      throw new HoodieIndexException("Error in reading " + fileSlice, e);
    }
  }

  private Option<FileSlice> getLatestFileSlice() {
    if (nonEmpty(instantTime)
        && hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant().isPresent()) {
      return Option.fromJavaOptional(hoodieTable
          .getHoodieView()
          .getLatestMergedFileSlicesBeforeOrOn(partitionPathFileIDPair.getLeft(), instantTime)
          .filter(fileSlice -> fileSlice.getFileId().equals(partitionPathFileIDPair.getRight()))
          .findFirst());
    }
    return Option.empty();
  }

  private Option<HoodieFileReader> getBaseFileReader(FileSlice fileSlice) throws IOException {
    if (fileSlice.getBaseFile().isPresent()) {
      return Option.of(createNewFileReader(fileSlice.getBaseFile().get()));
    }
    return Option.empty();
  }

  private HoodieMergedLogRecordScanner getLogRecordScanner(FileSlice fileSlice) {
    List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
        .map(l -> l.getPath().toString()).collect(toList());
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(hoodieTable.getMetaClient().getFs())
        .withMetaClient(hoodieTable.getMetaClient())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(instantTime)
        .withMaxMemorySizeInBytes(IOUtils.getMaxMemoryPerCompaction(hoodieTable.getTaskContextSupplier(), config))
        .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withPartition(fileSlice.getPartitionPath())
        .withOptimizedLogBlocksScan(config.enableOptimizedLogBlocksScan())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withRecordMerger(config.getRecordMerger())
        .build();
  }

  /**
   * Iterator that lazily merges records from base file and log files without
   * accumulating everything in memory.
   */
  private class MergedRecordsIterator implements ClosableIterator<HoodieRecord<T>> {
    private final Option<HoodieFileReader> baseFileReaderOpt;
    private final HoodieMergedLogRecordScanner logRecordScanner;
    private final HoodieRecordLocation currentLocation;
    private final Map<String, HoodieRecord> deltaRecordMap;
    private final Set<String> deltaRecordKeys;
    private final ClosableIterator<HoodieRecord<T>> baseFileIterator;
    private final HoodieRecordMerger recordMerger;
    private final Option<Pair<String, String>> simpleKeyGenFieldsOpt;

    private Iterator<String> remainingDeltaKeysIterator; // created lazily when Phase 2 starts
    private HoodieRecord<T> nextRecord;
    private boolean baseFilePhaseComplete = false;
    private boolean closed = false;

    MergedRecordsIterator(Option<HoodieFileReader> baseFileReaderOpt,
                          HoodieMergedLogRecordScanner logRecordScanner,
                          HoodieRecordLocation currentLocation) throws IOException {
      this.baseFileReaderOpt = baseFileReaderOpt;
      this.logRecordScanner = logRecordScanner;
      this.currentLocation = currentLocation;
      this.deltaRecordMap = logRecordScanner.getRecords();
      this.deltaRecordKeys = new HashSet<>(deltaRecordMap.keySet());
      this.recordMerger = config.getRecordMerger();

      HoodieTableConfig tableConfig = hoodieTable.getMetaClient().getTableConfig();
      this.simpleKeyGenFieldsOpt = tableConfig.populateMetaFields()
          ? Option.empty()
          : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(), tableConfig.getPartitionFieldProp()));

      if (baseFileReaderOpt.isPresent()) {
        this.baseFileIterator = baseFileReaderOpt.get().getRecordIterator(baseFileReaderSchema);
      } else {
        this.baseFileIterator = null;
        this.baseFilePhaseComplete = true;
      }

      advance();
    }

    @Override
    public boolean hasNext() {
      return nextRecord != null;
    }

    @Override
    public HoodieRecord<T> next() {
      if (nextRecord == null) {
        throw new java.util.NoSuchElementException();
      }
      HoodieRecord<T> result = nextRecord;
      advance();
      return result;
    }

    private void advance() {
      nextRecord = null;

      // Phase 1: Process base file records with merging
      if (!baseFilePhaseComplete && baseFileIterator != null) {
        while (baseFileIterator.hasNext() && nextRecord == null) {
          try {
            HoodieRecord<T> record = baseFileIterator.next().wrapIntoHoodieRecordPayloadWithParams(
                readerSchema, config.getProps(), simpleKeyGenFieldsOpt,
                logRecordScanner.isWithOperationField(), logRecordScanner.getPartitionNameOverride(),
                false, Option.empty());
            String key = record.getRecordKey();

            if (deltaRecordMap.containsKey(key)) {
              // Merge with delta record
              deltaRecordKeys.remove(key);
              Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger.merge(
                  record, readerSchema, deltaRecordMap.get(key), readerSchema,
                  config.getPayloadConfig().getProps());

              if (mergeResult.isPresent()) {
                HoodieRecord<T> mergedRecord = mergeResult.get().getLeft()
                    .wrapIntoHoodieRecordPayloadWithParams(
                        readerSchema, config.getProps(), simpleKeyGenFieldsOpt,
                        logRecordScanner.isWithOperationField(),
                        logRecordScanner.getPartitionNameOverride(), false, Option.empty());
                nextRecord = prepareRecord(mergedRecord);
              }
              // If merge result is not present, skip this record (it's been deleted)
            } else {
              // No delta record, use base record
              nextRecord = prepareRecord(record.copy());
            }
          } catch (Exception e) {
            close();
            throw new HoodieIndexException("Error processing base file record", e);
          }
        }

        // Base file iteration complete
        if (nextRecord == null) {
          baseFilePhaseComplete = true;
        }
      }

      // Phase 2: Process remaining delta-only records (iterator created after Phase 1 so set is correct)
      if (baseFilePhaseComplete && nextRecord == null) {
        if (remainingDeltaKeysIterator == null) {
          remainingDeltaKeysIterator = deltaRecordKeys.iterator();
        }
        while (remainingDeltaKeysIterator.hasNext() && nextRecord == null) {
          String key = remainingDeltaKeysIterator.next();
          HoodieRecord deltaRecord = deltaRecordMap.get(key);
          if (deltaRecord != null) {
            nextRecord = prepareRecord((HoodieRecord<T>) deltaRecord);
          }
        }
      }

      // If we've exhausted all records, close resources
      if (nextRecord == null && !closed) {
        close();
      }
    }

    private HoodieRecord<T> prepareRecord(HoodieRecord<T> record) {
      record.unseal();
      record.setCurrentLocation(currentLocation);
      record.seal();
      return record;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;

      if (baseFileIterator != null) {
        baseFileIterator.close();
      }
      if (baseFileReaderOpt.isPresent()) {
        baseFileReaderOpt.get().close();
      }
      if (logRecordScanner != null) {
        logRecordScanner.close();
      }
    }
  }
}

