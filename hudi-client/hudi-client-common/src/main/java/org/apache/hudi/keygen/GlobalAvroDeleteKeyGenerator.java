/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.RecordKeyGeneratorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Avro Key generator for deletes using global indices. Global index deletes do not require partition value so this key generator
 * avoids using partition value for generating HoodieKey.
 */
public class GlobalAvroDeleteKeyGenerator extends BaseKeyGenerator {

  private static final String EMPTY_PARTITION = "";
  private final RecordKeyGenerator recordKeyGenerator;

  public GlobalAvroDeleteKeyGenerator(TypedProperties config) {
    super(config);
    this.recordKeyFields = Arrays.asList(config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()).split(","));
    this.recordKeyGenerator = RecordKeyGeneratorFactory.getRecordKeyGenerator(config, recordKeyFields, isConsistentLogicalTimestampEnabled(), new ArrayList<>());
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return recordKeyGenerator.getRecordKey(record);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return EMPTY_PARTITION;
  }

  @Override
  public List<String> getPartitionPathFields() {
    return new ArrayList<>();
  }

  public String getEmptyPartition() {
    return EMPTY_PARTITION;
  }
}
