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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.DirectWriteMarkers;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.timeline.TimelineServiceClient;
import org.apache.hudi.timeline.TimelineServiceClientBase;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.marker.MarkerOperation.APPEND_MARKERS_URL;
import static org.apache.hudi.common.table.marker.MarkerOperation.MARKER_DIR_PATH_PARAM;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.table.action.rollback.BaseRollbackHelper.EMPTY_STRING;

public class RollbackUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RollbackUtils.class);

  /**
   * Get Latest version of Rollback plan corresponding to a clean instant.
   *
   * @param metaClient      Hoodie Table Meta Client
   * @param rollbackInstant Instant referring to rollback action
   * @return Rollback plan corresponding to rollback instant
   * @throws IOException
   */
  public static HoodieRollbackPlan getRollbackPlan(HoodieTableMetaClient metaClient, HoodieInstant rollbackInstant)
      throws IOException {
    // TODO: add upgrade step if required.
    final HoodieInstant requested = metaClient.getInstantGenerator().getRollbackRequestedInstant(rollbackInstant);
    return metaClient.getActiveTimeline().readRollbackPlan(requested);
  }

  static Map<HoodieLogBlock.HeaderMetadataType, String> generateHeader(String instantToRollback, String rollbackInstantTime) {
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, rollbackInstantTime);
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, instantToRollback);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    return header;
  }

  /**
   * Helper to merge 2 rollback-stats for a given partition.
   *
   * @param stat1 HoodieRollbackStat
   * @param stat2 HoodieRollbackStat
   * @return Merged HoodieRollbackStat
   */
  static HoodieRollbackStat mergeRollbackStat(HoodieRollbackStat stat1, HoodieRollbackStat stat2) {
    checkArgument(stat1.getPartitionPath().equals(stat2.getPartitionPath()));
    final List<String> successDeleteFiles = new ArrayList<>();
    final List<String> failedDeleteFiles = new ArrayList<>();
    final Map<StoragePathInfo, Long> commandBlocksCount = new HashMap<>();
    final Map<String, Long> logFilesFromFailedCommit = new HashMap<>();
    Option.ofNullable(stat1.getSuccessDeleteFiles()).ifPresent(successDeleteFiles::addAll);
    Option.ofNullable(stat2.getSuccessDeleteFiles()).ifPresent(successDeleteFiles::addAll);
    Option.ofNullable(stat1.getFailedDeleteFiles()).ifPresent(failedDeleteFiles::addAll);
    Option.ofNullable(stat2.getFailedDeleteFiles()).ifPresent(failedDeleteFiles::addAll);
    Option.ofNullable(stat1.getCommandBlocksCount()).ifPresent(commandBlocksCount::putAll);
    Option.ofNullable(stat2.getCommandBlocksCount()).ifPresent(commandBlocksCount::putAll);
    Option.ofNullable(stat1.getLogFilesFromFailedCommit()).ifPresent(logFilesFromFailedCommit::putAll);
    Option.ofNullable(stat2.getLogFilesFromFailedCommit()).ifPresent(logFilesFromFailedCommit::putAll);
    return new HoodieRollbackStat(stat1.getPartitionPath(), successDeleteFiles, failedDeleteFiles, commandBlocksCount, logFilesFromFailedCommit);
  }

  static List<HoodieRollbackRequest> groupRollbackRequestsBasedOnFileGroup(List<HoodieRollbackRequest> rollbackRequests) {
    return groupRollbackRequestsBasedOnFileGroup(rollbackRequests, e -> e,
        HoodieRollbackRequest::getPartitionPath,
        HoodieRollbackRequest::getFileId,
        HoodieRollbackRequest::getLatestBaseInstant,
        HoodieRollbackRequest::getLogBlocksToBeDeleted,
        HoodieRollbackRequest::getFilesToBeDeleted);
  }

  static List<SerializableHoodieRollbackRequest> groupSerializableRollbackRequestsBasedOnFileGroup(
      List<SerializableHoodieRollbackRequest> rollbackRequests) {
    return groupRollbackRequestsBasedOnFileGroup(rollbackRequests, SerializableHoodieRollbackRequest::new,
        SerializableHoodieRollbackRequest::getPartitionPath,
        SerializableHoodieRollbackRequest::getFileId,
        SerializableHoodieRollbackRequest::getLatestBaseInstant,
        SerializableHoodieRollbackRequest::getLogBlocksToBeDeleted,
        SerializableHoodieRollbackRequest::getFilesToBeDeleted);
  }

  /**
   * Groups the rollback requests so that each file group has at most two non-empty rollback requests:
   * one for base file, the other for all log files to be rolled back.
   *
   * @param rollbackRequests            input rollback request list
   * @param createRequestFunc           function to instantiate T object
   * @param getPartitionPathFunc        function to get partition path from the rollback request
   * @param getFileIdFunc               function to get file ID from the rollback request
   * @param getLatestBaseInstant        function to get the latest base instant time from the rollback request
   * @param getLogBlocksToBeDeletedFunc function to get log blocks to be deleted from the rollback request
   * @param getFilesToBeDeletedFunc     function to get files to be deleted from the rollback request
   * @param <T>                         should be either {@link HoodieRollbackRequest} or {@link SerializableHoodieRollbackRequest}
   * @return a list of rollback requests after grouping
   */
  static <T> List<T> groupRollbackRequestsBasedOnFileGroup(List<T> rollbackRequests,
                                                           Function<HoodieRollbackRequest, T> createRequestFunc,
                                                           Function<T, String> getPartitionPathFunc,
                                                           Function<T, String> getFileIdFunc,
                                                           Function<T, String> getLatestBaseInstant,
                                                           Function<T, Map<String, Long>> getLogBlocksToBeDeletedFunc,
                                                           Function<T, List<String>> getFilesToBeDeletedFunc) {
    // Grouping the rollback requests to a map of pairs of partition and file ID to a list of rollback requests
    Map<Pair<String, String>, List<T>> requestMap = new HashMap<>();
    rollbackRequests.forEach(rollbackRequest -> {
      String partitionPath = getPartitionPathFunc.apply(rollbackRequest);
      Pair<String, String> partitionFileIdPair =
          Pair.of(partitionPath != null ? partitionPath : "", getFileIdFunc.apply(rollbackRequest));
      requestMap.computeIfAbsent(partitionFileIdPair, k -> new ArrayList<>()).add(rollbackRequest);
    });
    return requestMap.entrySet().stream().flatMap(entry -> {
      List<T> rollbackRequestList = entry.getValue();
      List<T> newRequestList = new ArrayList<>();
      // Group all log blocks to be deleted in one file group together in a new rollback request
      Map<String, Long> logBlocksToBeDeleted = new HashMap<>();
      rollbackRequestList.forEach(rollbackRequest -> {
        if (!getLogBlocksToBeDeletedFunc.apply(rollbackRequest).isEmpty()) {
          // For rolling back log blocks by appending rollback log blocks
          if (!getFilesToBeDeletedFunc.apply(rollbackRequest).isEmpty()) {
            // This should never happen based on the rollback request generation
            // As a defensive guard, adding the files to be deleted to a new rollback request
            LOG.warn("Only one of the following should be non-empty. "
                    + "Adding the files to be deleted to a new rollback request. "
                    + "FilesToBeDeleted: {}, LogBlocksToBeDeleted: {}",
                getFilesToBeDeletedFunc.apply(rollbackRequest),
                getLogBlocksToBeDeletedFunc.apply(rollbackRequest));
            String partitionPath = getPartitionPathFunc.apply(rollbackRequest);
            newRequestList.add(createRequestFunc.apply(HoodieRollbackRequest.newBuilder()
                .setPartitionPath(partitionPath != null ? partitionPath : "")
                .setFileId(getFileIdFunc.apply(rollbackRequest))
                .setLatestBaseInstant(getLatestBaseInstant.apply(rollbackRequest))
                .setFilesToBeDeleted(getFilesToBeDeletedFunc.apply(rollbackRequest))
                .setLogBlocksToBeDeleted(Collections.emptyMap())
                .build()));
          }
          logBlocksToBeDeleted.putAll(getLogBlocksToBeDeletedFunc.apply(rollbackRequest));
        } else {
          // For base or log files to delete or empty rollback request
          newRequestList.add(rollbackRequest);
        }
      });
      if (!logBlocksToBeDeleted.isEmpty() && !rollbackRequestList.isEmpty()) {
        // Generating a new rollback request for all log files in the same file group
        newRequestList.add(createRequestFunc.apply(HoodieRollbackRequest.newBuilder()
            .setPartitionPath(entry.getKey().getKey())
            .setFileId(entry.getKey().getValue())
            .setLatestBaseInstant(getLatestBaseInstant.apply(rollbackRequestList.get(0)))
            .setFilesToBeDeleted(Collections.emptyList())
            .setLogBlocksToBeDeleted(logBlocksToBeDeleted)
            .build()));
      }
      return newRequestList.stream();
    }).collect(Collectors.toList());
  }

  static void getRollbackRequestsForCompactionDuringRestore(HoodieInstant instantToRollback, String partitionPath, HoodieTableMetaClient metaClient, List<HoodieRollbackRequest> hoodieRollbackRequests,
                                                            Supplier<List<StoragePathInfo>> filesToDelete, String baseFileExtension) throws IOException {
    if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      // For table version 6, the files can be directly fetched from the instant to rollback
      hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath, filesToDelete.get()));
    } else {
      // For table version 8, the files are computed based on completion time. All files completed after
      // the requested time of instant to rollback are included
      hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath,
          listAllFilesSinceCommit(instantToRollback.requestedTime(), baseFileExtension, partitionPath,
              metaClient)));
    }
  }

  public static List<HoodieRollbackRequest> getRollbackRequestToAppendForVersionSix(String partitionPath, HoodieInstant rollbackInstant,
                                                                                    HoodieCommitMetadata commitMetadata, HoodieTable<?, ?, ?, ?> table) {
    List<HoodieRollbackRequest> hoodieRollbackRequests =  new ArrayList<>();
    checkArgument(table.version().lesserThan(HoodieTableVersion.EIGHT));
    checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the base and not the requested, hence get the
    // baseCommit always by listing the file slice
    // With multi writers, rollbacks could be lazy. and so we need to use getLatestFileSlicesBeforeOrOn() instead of getLatestFileSlices()
    Map<String, FileSlice> latestFileSlices = table.getSliceView()
        .getLatestFileSlicesBeforeOrOn(partitionPath, rollbackInstant.requestedTime(), true)
        .collect(Collectors.toMap(FileSlice::getFileId, Function.identity()));

    List<HoodieWriteStat> hoodieWriteStats = Option.ofNullable(commitMetadata.getPartitionToWriteStats().get(partitionPath)).orElse(Collections.emptyList());
    hoodieWriteStats = hoodieWriteStats.stream()
        .filter(writeStat -> {
          // Filter out stats without prevCommit since they are all inserts
          boolean validForRollback = (writeStat != null) && (!writeStat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT))
              && (writeStat.getPrevCommit() != null) && latestFileSlices.containsKey(writeStat.getFileId());

          if (!validForRollback) {
            return false;
          }

          FileSlice latestFileSlice = latestFileSlices.get(writeStat.getFileId());

          // For sanity, log-file base-instant time can never be less than base-commit on which we are rolling back
          checkArgument(
              compareTimestamps(latestFileSlice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, rollbackInstant.requestedTime()),
              "Log-file base-instant could not be less than the instant being rolled back");

          // Command block "rolling back" the preceding block {@link HoodieCommandBlockTypeEnum#ROLLBACK_PREVIOUS_BLOCK}
          // w/in the latest file-slice is appended iff base-instant of the log-file is _strictly_ less
          // than the instant of the Delta Commit being rolled back. Otherwise, log-file will be cleaned up
          // in a different branch of the flow.
          return compareTimestamps(latestFileSlice.getBaseInstantTime(), LESSER_THAN, rollbackInstant.requestedTime());
        })
        .collect(Collectors.toList());

    for (HoodieWriteStat writeStat : hoodieWriteStats.stream().filter(
        hoodieWriteStat -> !StringUtils.isNullOrEmpty(hoodieWriteStat.getFileId())).collect(Collectors.toList())) {
      FileSlice latestFileSlice = latestFileSlices.get(writeStat.getFileId());
      String fileId = writeStat.getFileId();
      String latestBaseInstant = latestFileSlice.getBaseInstantTime();
      Path fullLogFilePath = HadoopFSUtils.constructAbsolutePathInHadoopPath(table.getConfig().getBasePath(), writeStat.getPath());
      Map<String, Long> logFilesWithBlocksToRollback = Collections.singletonMap(
          fullLogFilePath.toString(), writeStat.getTotalWriteBytes() > 0 ? writeStat.getTotalWriteBytes() : 1L);
      hoodieRollbackRequests.add(new HoodieRollbackRequest(partitionPath, fileId, latestBaseInstant,
          Collections.emptyList(), logFilesWithBlocksToRollback));
    }
    return hoodieRollbackRequests;
  }

  private static List<StoragePathInfo> listAllFilesSinceCommit(String commit,
                                                               String baseFileExtension,
                                                               String partitionPath,
                                                               HoodieTableMetaClient metaClient) throws IOException {
    LOG.info("Collecting files to be cleaned/rolledback up for path " + partitionPath + " and commit " + commit);
    CompletionTimeQueryView completionTimeQueryView = metaClient.getTimelineLayout().getTimelineFactory().createCompletionTimeQueryView(metaClient);
    StoragePathFilter filter = (path) -> {
      if (path.toString().contains(baseFileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return compareTimestamps(commit, LESSER_THAN_OR_EQUALS,
            fileCommitTime);
      } else if (FSUtils.isLogFile(path)) {
        String fileCommitTime = FSUtils.getDeltaCommitTimeFromLogPath(path);
        return completionTimeQueryView.isSlicedAfterOrOn(commit, fileCommitTime);
      }
      return false;
    };
    return metaClient.getStorage()
        .listDirectEntries(FSUtils.constructAbsolutePath(metaClient.getBasePath(), partitionPath), filter);
  }

  @NotNull
  static List<HoodieRollbackRequest> getHoodieRollbackRequests(String partitionPath, List<StoragePathInfo> filesToDeletedStatus) {
    return filesToDeletedStatus.stream()
        .map(pathInfo -> {
          String dataFileToBeDeleted = pathInfo.getPath().toString();
          return formatDeletePath(dataFileToBeDeleted);
        })
        .map(s -> new HoodieRollbackRequest(partitionPath, EMPTY_STRING, EMPTY_STRING, Collections.singletonList(s), Collections.emptyMap()))
        .collect(Collectors.toList());
  }

  private static String formatDeletePath(String path) {
    // strip scheme E.g: file:/var/folders
    return path.substring(path.indexOf(":") + 1);
  }

  static void getRollbackRequestsForDeltaCommit(HoodieInstant instantToRollback, String partitionPath, List<HoodieRollbackRequest> hoodieRollbackRequests,
                                                Supplier<List<StoragePathInfo>> filesToDelete, HoodieTableMetaClient metaClient,
                                                Option<HoodieCommitMetadata> commitMetadataOptional, HoodieTable<?, ?, ?, ?> table) {
    // In case all data was inserts and the commit failed, delete the file belonging to that commit
    // We do not know fileIds for inserts (first inserts are either log files or base files),
    // delete all files for the corresponding failed commit, if present (same as COW)
    hoodieRollbackRequests.addAll(getHoodieRollbackRequests(partitionPath, filesToDelete.get()));
    if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {

      // --------------------------------------------------------------------------------------------------
      // (A) The following cases are possible if index.canIndexLogFiles and/or index.isGlobal
      // --------------------------------------------------------------------------------------------------
      // (A.1) Failed first commit - Inserts were written to log files and HoodieWriteStat has no entries. In
      // this scenario we would want to delete these log files.
      // (A.2) Failed recurring commit - Inserts/Updates written to log files. In this scenario,
      // HoodieWriteStat will have the baseCommitTime for the first log file written, add rollback blocks.
      // (A.3) Rollback triggered for first commit - Inserts were written to the log files but the commit is
      // being reverted. In this scenario, HoodieWriteStat will be `null` for the attribute prevCommitTime
      // and hence will end up deleting these log files. This is done so there are no orphan log files
      // lying around.
      // (A.4) Rollback triggered for recurring commits - Inserts/Updates are being rolled back, the actions
      // taken in this scenario is a combination of (A.2) and (A.3)
      // ---------------------------------------------------------------------------------------------------
      // (B) The following cases are possible if !index.canIndexLogFiles and/or !index.isGlobal
      // ---------------------------------------------------------------------------------------------------
      // (B.1) Failed first commit - Inserts were written to base files and HoodieWriteStat has no entries.
      // In this scenario, we delete all the base files written for the failed commit.
      // (B.2) Failed recurring commits - Inserts were written to base files and updates to log files. In
      // this scenario, perform (A.1) and for updates written to log files, write rollback blocks.
      // (B.3) Rollback triggered for first commit - Same as (B.1)
      // (B.4) Rollback triggered for recurring commits - Same as (B.2) plus we need to delete the log files
      // as well if the base file gets deleted.
      HoodieCommitMetadata commitMetadata = commitMetadataOptional.get();
      if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
        hoodieRollbackRequests.addAll(getRollbackRequestToAppendForVersionSix(partitionPath, instantToRollback, commitMetadata, table));
      }
    }
  }

  /**
   * Fetches markers for log files w/ Append IOType. Used only for table version 6.
   * @param markers WriteMarkers
   * @param context {@code HoodieEngineContext} instance.
   * @param parallelism parallelism for reading the marker files in the directory.
   * @return all the log file paths of write IO type "APPEND"
   * @throws IOException
   */
  public static Set<String> getAppendedLogPaths(WriteMarkers markers, HoodieEngineContext context, int parallelism) throws IOException {
    if (markers instanceof TimelineServerBasedWriteMarkers) {
      Map<String, String> paramsMap = Collections.singletonMap(MARKER_DIR_PATH_PARAM, markers.getMarkerDirPath().toString());
      try {
        Set<String> markerPaths = executeRequestToTimelineServer(((TimelineServerBasedWriteMarkers)markers).getTimelineServiceClient(),
            APPEND_MARKERS_URL, paramsMap, new TypeReference<Set<String>>() {}, TimelineServiceClientBase.RequestMethod.GET);
        return markerPaths.stream().map(WriteMarkers::stripMarkerSuffix).collect(Collectors.toSet());
      } catch (IOException e) {
        throw new HoodieRemoteException("Failed to get APPEND log file paths in "
            + markers.getMarkerDirPath().toString(), e);
      }
    } else {
      StorageConfiguration storageConf = context.getStorageConf();
      HoodieStorage storage = HoodieStorageUtils.getStorage(markers.getMarkerDirPath(), storageConf);
      Pair<List<String>, Set<String>> subDirectoriesAndDataFiles = DirectWriteMarkers.getSubDirectoriesByMarkerCondition(storage.listDirectEntries(markers.getMarkerDirPath()),
          getAppendMarkerPredicate(), markers);
      List<String> subDirectories = subDirectoriesAndDataFiles.getLeft();
      Set<String> logFiles = subDirectoriesAndDataFiles.getRight();
      if (subDirectories.size() > 0) {
        parallelism = Math.min(subDirectories.size(), parallelism);
        context.setJobStatus(DirectWriteMarkers.class.getSimpleName(), "Obtaining marker files for all created, merged paths");
        logFiles.addAll(context.flatMap(subDirectories, directory -> {
          Queue<StoragePath> candidatesDirs = new LinkedList<>();
          candidatesDirs.add(new StoragePath(directory));
          List<String> result = new ArrayList<>();
          while (!candidatesDirs.isEmpty()) {
            StoragePath path = candidatesDirs.remove();
            HoodieStorage markerStorage = HoodieStorageUtils.getStorage(markers.getMarkerDirPath(), storageConf);
            List<StoragePathInfo> storagePathInfos = markerStorage.listDirectEntries(path);
            for (StoragePathInfo pathInfo : storagePathInfos) {
              if (pathInfo.isDirectory()) {
                candidatesDirs.add(pathInfo.getPath());
              } else {
                String pathStr = pathInfo.getPath().toString();
                if (getAppendMarkerPredicate().test(pathStr)) {
                  result.add(translateMarkerToDataPath(pathStr, markers));
                }
              }
            }
          }
          return result.stream();
        }, parallelism));
      }

      return logFiles;
    }
  }

  private static Predicate<String> getAppendMarkerPredicate() {
    return pathStr -> pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && pathStr.endsWith(IOType.APPEND.name());
  }

  public static String translateMarkerToDataPath(String markerPath, WriteMarkers markers) {
    String rPath = MarkerUtils.stripMarkerFolderPrefix(markerPath, markers.getBasePath(), markers.getInstantTime());
    return WriteMarkers.stripMarkerSuffix(rPath);
  }

  private static <T> T executeRequestToTimelineServer(TimelineServiceClient timelineServiceClient, String requestPath, Map<String, String> queryParameters,
                                               TypeReference reference, TimelineServiceClientBase.RequestMethod method) throws IOException {
    return timelineServiceClient.makeRequest(
            TimelineServiceClient.Request.newBuilder(method, requestPath).addQueryParams(queryParameters).build())
        .getDecodedContent(reference);
  }
}
