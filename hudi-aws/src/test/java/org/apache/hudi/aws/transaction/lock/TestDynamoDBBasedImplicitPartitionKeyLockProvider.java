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

package org.apache.hudi.aws.transaction.lock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDynamoDBBasedImplicitPartitionKeyLockProvider {

  // ENG-41644 regression: a Cardlytics customerofferserved_v2 table was acquiring two different
  // DynamoDB lock rows because the dataplane Spark job passed the basePath with a trailing slash
  // (key AB165642952B6C0C) while the spark-thrift-server passed it without (key 921119D2D8532C9C).
  // After normalization both forms must produce the same partition key — the with-slash key.
  private static final String CARDLYTICS_BASE_PATH_WITH_SLASH =
      "s3://dp-datalake-gold-use1-prod/Domain=OfferServed/Table=customerofferserved_v2/";
  private static final String CARDLYTICS_BASE_PATH_NO_SLASH =
      "s3://dp-datalake-gold-use1-prod/Domain=OfferServed/Table=customerofferserved_v2";
  // = HashID.generateXXHashAsString(CARDLYTICS_BASE_PATH_WITH_SLASH, HashID.Size.BITS_64).
  // Locked in by the Cardlytics production incident; if HashID's XXH64 implementation or seed
  // ever changes this constant must be regenerated and the rollout caveat in the PR description
  // re-evaluated.
  private static final String CARDLYTICS_CANONICAL_PARTITION_KEY = "AB165642952B6C0C";

  @Test
  void trailingSlashVariantsProduceSamePartitionKey() {
    String withSlash = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CARDLYTICS_BASE_PATH_WITH_SLASH);
    String noSlash = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CARDLYTICS_BASE_PATH_NO_SLASH);
    Assertions.assertEquals(CARDLYTICS_CANONICAL_PARTITION_KEY, withSlash);
    Assertions.assertEquals(CARDLYTICS_CANONICAL_PARTITION_KEY, noSlash);
  }

  @Test
  void multipleTrailingSlashesProduceSamePartitionKey() {
    String once = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CARDLYTICS_BASE_PATH_WITH_SLASH);
    String twice = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CARDLYTICS_BASE_PATH_NO_SLASH + "//");
    String thrice = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CARDLYTICS_BASE_PATH_NO_SLASH + "///");
    Assertions.assertEquals(once, twice);
    Assertions.assertEquals(once, thrice);
  }

  @Test
  void surroundingWhitespaceProducesSamePartitionKey() {
    String clean = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CARDLYTICS_BASE_PATH_WITH_SLASH);
    String padded = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey("  " + CARDLYTICS_BASE_PATH_NO_SLASH + "  ");
    Assertions.assertEquals(clean, padded);
  }

  @Test
  void s3aSchemeProducesSamePartitionKeyAsS3() {
    String s3 = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(CARDLYTICS_BASE_PATH_WITH_SLASH);
    String s3a = DynamoDBBasedImplicitPartitionKeyLockProvider.derivePartitionKey(
        CARDLYTICS_BASE_PATH_WITH_SLASH.replaceFirst("^s3://", "s3a://"));
    Assertions.assertEquals(s3, s3a);
  }
}
