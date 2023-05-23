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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link Transformer} to chain other {@link Transformer}s and apply sequentially.
 */
public class ChainedTransformer implements Transformer {

  // Delimiter used to separate class name and the property key suffix. The suffix comes first.
  private static final String ID_TRANSFORMER_CLASS_NAME_DELIMITER = ":";

  protected final List<TransformerInfo> transformers;
  private final Option<Schema> sourceSchemaOpt;
  private final boolean enableSchemaValidation;

  public ChainedTransformer(List<Transformer> transformersList) {
    this.transformers = new ArrayList<>(transformersList.size());
    for (Transformer transformer : transformersList) {
      this.transformers.add(new TransformerInfo(transformer));
    }
    this.sourceSchemaOpt = Option.empty();
    this.enableSchemaValidation = false;
  }

  /**
   * Creates a chained transformer using the input transformer class names. Refer {@link HoodieDeltaStreamer.Config#transformerClassNames}
   * for more information on how the transformers can be configured.
   *
   * @param sourceSchemaOpt                   Schema from the dataset the transform is applied to
   * @param configuredTransformers            List of configured transformer class names.
   * @param enableSchemaValidation            If true, schema is validated for the transformed data against expected schema.
   *                                          Expected schema is provided by {@link Transformer#transformedSchema}
   */
  public ChainedTransformer(List<String> configuredTransformers, Option<Schema> sourceSchemaOpt, boolean enableSchemaValidation) {
    this.transformers = new ArrayList<>(configuredTransformers.size());
    this.enableSchemaValidation = enableSchemaValidation;
    this.sourceSchemaOpt = sourceSchemaOpt;

    Set<String> identifiers = new HashSet<>();
    for (String configuredTransformer : configuredTransformers) {
      if (!configuredTransformer.contains(ID_TRANSFORMER_CLASS_NAME_DELIMITER)) {
        transformers.add(new TransformerInfo(ReflectionUtils.loadClass(configuredTransformer)));
      } else {
        String[] splits = configuredTransformer.split(ID_TRANSFORMER_CLASS_NAME_DELIMITER);
        if (splits.length > 2) {
          throw new HoodieTransformPlanException("There should only be one colon in a configured transformer");
        }
        String id = splits[0];
        validateIdentifier(id, identifiers, configuredTransformer);
        Transformer transformer = ReflectionUtils.loadClass(splits[1]);
        transformers.add(new TransformerInfo(transformer, id));
      }
    }
    if (!(transformers.stream().allMatch(TransformerInfo::hasIdentifier)
        || transformers.stream().noneMatch(TransformerInfo::hasIdentifier))) {
      throw new HoodieTransformPlanException("Either all transformers should have identifier or none should");
    }
  }

  public List<String> getTransformersNames() {
    return transformers.stream().map(t -> t.getTransformer().getClass().getName()).collect(Collectors.toList());
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
    Dataset<Row> dataset = rowDataset;
    Option<StructType> incomingStructOpt;
    if (!sourceSchemaOpt.isPresent()) {
      incomingStructOpt = Option.of(rowDataset.schema());
    } else {
      incomingStructOpt = Option.of(AvroConversionUtils.convertAvroSchemaToStructType(sourceSchemaOpt.get()));
    }
    for (TransformerInfo transformerInfo : transformers) {
      Transformer transformer = transformerInfo.getTransformer();
      dataset = transformer.apply(jsc, sparkSession, dataset, transformerInfo.getProperties(properties, transformers));
      if (enableSchemaValidation) {
        // Transformed schema of 1st transformer is incoming schema for 2nd transformer
        incomingStructOpt = validateAndGetTransformedSchema(transformerInfo, dataset, incomingStructOpt, jsc, sparkSession, properties);
      }
    }
    return dataset;
  }

  private void validateIdentifier(String id, Set<String> identifiers, String configuredTransformer) {
    if (StringUtils.isNullOrEmpty(id)) {
      throw new HoodieTransformPlanException(String.format("Transformer identifier is empty for %s", configuredTransformer));
    }
    if (identifiers.contains(id)) {
      throw new HoodieTransformPlanException(String.format("Duplicate identifier %s found for transformer %s", id, configuredTransformer));
    } else {
      identifiers.add(id);
    }
  }

  private Option<StructType> validateAndGetTransformedSchema(TransformerInfo transformerInfo, Dataset<Row> dataset, Option<StructType> incomingStructOpt,
                                                             JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties) {
    if (!incomingStructOpt.isPresent()) {
      throw new HoodieSchemaException(String.format("Source schema not available for transformer %s", transformerInfo));
    }
    StructType incomingStruct = incomingStructOpt.get();

    // Get expected target schema from transformer and actual target schema from struct
    StructType targetStruct = dataset.schema();
    Option<StructType> expectedTargetStructOpt = transformerInfo.getTransformer().transformedSchema(jsc, sparkSession, incomingStruct, properties);
    if (!expectedTargetStructOpt.isPresent()) {
      throw new HoodieSchemaException(String.format("Expected target schema not provided for transformer %s", transformerInfo));
    }

    StructType expectedTargetStruct = expectedTargetStructOpt.get().asNullable();
    if (!expectedTargetStruct.equals(targetStruct)) {
      throw new HoodieSchemaException(String.format("Schema of transformed data does not match expected schema for transformer %s\nexpected=%s \nactual=%s",
          transformerInfo, expectedTargetStruct, targetStruct));
    }
    return expectedTargetStructOpt;
  }

  protected static class TransformerInfo {
    private final Transformer transformer;
    private final Option<String> idOpt;

    private TransformerInfo(Transformer transformer, String id) {
      this.transformer = transformer;
      this.idOpt = Option.of(id);
    }

    private TransformerInfo(Transformer transformer) {
      this.transformer = transformer;
      this.idOpt = Option.empty();
    }

    protected Transformer getTransformer() {
      return transformer;
    }

    private boolean hasIdentifier() {
      return idOpt.isPresent();
    }

    protected TypedProperties getProperties(TypedProperties properties, List<TransformerInfo> transformers) {
      Set<String> transformerIds = transformers.stream().map(transformerInfo -> transformerInfo.idOpt.orElse(null))
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
      TypedProperties transformerProps = properties;
      if (idOpt.isPresent()) {
        // Transformer specific property keys end with the id associated with the transformer.
        // Ex. For id tr1, key `hoodie.deltastreamer.transformer.sql.tr1` would be converted to
        // `hoodie.deltastreamer.transformer.sql` and then passed to the transformer.
        String id = idOpt.get();
        transformerProps = new TypedProperties(properties);
        Map<String, Object> overrideKeysMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
          String key = (String) entry.getKey();
          String keyId = key.replaceAll(".*\\.", "");
          if (keyId.equals(id)) {
            overrideKeysMap.put(key.substring(0, key.length() - (id.length() + 1)), entry.getValue());
          }
          if (transformerIds.contains(keyId)) {
            transformerProps.remove(key);
          }
        }
        transformerProps.putAll(overrideKeysMap);
      }

      return transformerProps;
    }

    @Override
    public String toString() {
      return transformer + (idOpt.isPresent() ? idOpt.get() : "");
    }
  }
}
