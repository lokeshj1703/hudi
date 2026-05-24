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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ExternalSpillableMap.DiskMapType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConfigUtils {

  @Test
  public void testToMapSucceeds() {
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("k.1.1.2", "v1");
    expectedMap.put("k.2.1.2", "v2");
    expectedMap.put("k.3.1.2", "v3");

    // Test base case
    String srcKv = "k.1.1.2=v1\nk.2.1.2=v2\nk.3.1.2=v3";
    Map<String, String> outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test ends with new line
    srcKv = "k.1.1.2=v1\nk.2.1.2=v2\nk.3.1.2=v3\n";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines
    srcKv = "k.1.1.2=v1\nk.2.1.2=v2\n\nk.3.1.2=v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines with spaces in between
    srcKv = "k.1.1.2=v1\n  \nk.2.1.2=v2\n\nk.3.1.2=v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test with random spaces if trim works properly
    srcKv = " k.1.1.2 =   v1\n k.2.1.2 = v2 \nk.3.1.2 = v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);
  }

  @Test
  void testGetRawValueWithAltKeys() {
    TypedProperties properties = new TypedProperties();
    DiskMapType diskMapType = ConfigUtils.getRawValueWithAltKeys(properties, HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE, true);
    Assertions.assertEquals(DiskMapType.BITCASK, diskMapType);
    properties.put(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), DiskMapType.ROCKS_DB);
    diskMapType = ConfigUtils.getRawValueWithAltKeys(properties, HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE, true);
    Assertions.assertEquals(DiskMapType.ROCKS_DB, diskMapType);
    properties.remove(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key());
    Assertions.assertThrows(IllegalArgumentException.class, () -> ConfigUtils.getRawValueWithAltKeys(properties, HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE, false));
  }

  @Test
  public void testToMapThrowError() {
    String srcKv = "k.1.1.2=v1=v1.1\nk.2.1.2=v2\nk.3.1.2=v3";
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.toMap(srcKv));
  }

  @Test
  public void testExtractWithPrefixMatchesAndStrips() {
    Properties src = new Properties();
    src.setProperty("hoodie.metadata.writer.hoodie.filesystem.view.type", "SPILLABLE_DISK");
    src.setProperty("hoodie.metadata.writer.hoodie.embed.timeline.server", "false");
    src.setProperty("hoodie.base.path", "/tmp");
    List<String> dropped = new ArrayList<>();
    List<String> skippedEmpty = new ArrayList<>();

    Properties out = ConfigUtils.extractWithPrefix(src, "hoodie.metadata.writer.", Collections.emptySet(), dropped, skippedEmpty);

    assertEquals(2, out.size());
    assertEquals("SPILLABLE_DISK", out.getProperty("hoodie.filesystem.view.type"));
    assertEquals("false", out.getProperty("hoodie.embed.timeline.server"));
    assertTrue(dropped.isEmpty());
    assertTrue(skippedEmpty.isEmpty());
  }

  @Test
  public void testExtractWithPrefixNoMatch() {
    Properties src = new Properties();
    src.setProperty("hoodie.base.path", "/tmp");
    src.setProperty("hoodie.filesystem.view.type", "MEMORY");
    List<String> dropped = new ArrayList<>();
    List<String> skippedEmpty = new ArrayList<>();

    Properties out = ConfigUtils.extractWithPrefix(src, "hoodie.metadata.writer.", Collections.emptySet(), dropped, skippedEmpty);

    assertTrue(out.isEmpty());
    assertTrue(dropped.isEmpty());
  }

  @Test
  public void testExtractWithPrefixDropsBlockedKeys() {
    Properties src = new Properties();
    src.setProperty("hoodie.metadata.writer.hoodie.table.name", "evil_name");
    src.setProperty("hoodie.metadata.writer.hoodie.filesystem.view.type", "SPILLABLE_DISK");
    Set<String> blocklist = new HashSet<>();
    blocklist.add("hoodie.table.name");
    List<String> dropped = new ArrayList<>();
    List<String> skippedEmpty = new ArrayList<>();

    Properties out = ConfigUtils.extractWithPrefix(src, "hoodie.metadata.writer.", blocklist, dropped, skippedEmpty);

    assertEquals(1, out.size());
    assertEquals("SPILLABLE_DISK", out.getProperty("hoodie.filesystem.view.type"));
    assertFalse(out.containsKey("hoodie.table.name"));
    assertEquals(1, dropped.size());
    assertEquals("hoodie.table.name", dropped.get(0));
  }

  @Test
  public void testExtractWithPrefixSkipsEmptyValues() {
    Properties src = new Properties();
    src.setProperty("hoodie.metadata.writer.hoodie.knob.a", "");
    src.setProperty("hoodie.metadata.writer.hoodie.knob.b", "value");
    List<String> dropped = new ArrayList<>();
    List<String> skippedEmpty = new ArrayList<>();

    Properties out = ConfigUtils.extractWithPrefix(src, "hoodie.metadata.writer.", Collections.emptySet(), dropped, skippedEmpty);

    assertEquals(1, out.size());
    assertEquals("value", out.getProperty("hoodie.knob.b"));
    assertEquals(1, skippedEmpty.size());
    assertEquals("hoodie.knob.a", skippedEmpty.get(0));
  }

  @Test
  public void testExtractWithPrefixEmptyOrNullInputs() {
    List<String> dropped = new ArrayList<>();
    List<String> skippedEmpty = new ArrayList<>();

    assertTrue(ConfigUtils.extractWithPrefix(new Properties(), "hoodie.metadata.writer.", Collections.emptySet(), dropped, skippedEmpty).isEmpty());
    assertTrue(ConfigUtils.extractWithPrefix(null, "hoodie.metadata.writer.", Collections.emptySet(), dropped, skippedEmpty).isEmpty());

    Properties src = new Properties();
    src.setProperty("hoodie.metadata.writer.x", "v");
    assertTrue(ConfigUtils.extractWithPrefix(src, "", Collections.emptySet(), dropped, skippedEmpty).isEmpty());
    assertTrue(ConfigUtils.extractWithPrefix(src, null, Collections.emptySet(), dropped, skippedEmpty).isEmpty());
  }
}