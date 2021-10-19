/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.service;

import static feast.common.models.FeatureTable.getFeatureTableStringRef;

import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import feast.common.models.FeatureV2;
import feast.proto.core.DataSourceProto.DataSource;
import feast.proto.core.DataSourceProto.DataSource.RequestDataOptions;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureViewProto.FeatureView;
import feast.proto.core.FeatureViewProto.FeatureViewSpec;
import feast.proto.core.OnDemandFeatureViewProto.OnDemandFeatureViewSpec;
import feast.proto.core.OnDemandFeatureViewProto.OnDemandInput;
import feast.proto.serving.ServingAPIProto.FeastServingType;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.proto.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesRequest;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesResponse;
import feast.proto.serving.TransformationServiceAPIProto.ValueType;
import feast.proto.serving.TransformationServiceGrpc;
import feast.proto.types.ValueProto;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.specs.FeatureSpecRetriever;
import feast.serving.util.Metrics;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import io.grpc.*;
import io.grpc.Status;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.io.*;
import java.nio.channels.Channels;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tomcat.util.http.fileupload.ByteArrayOutputStream;
import org.slf4j.Logger;

public class OnlineServingServiceV2 implements ServingServiceV2 {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineServingServiceV2.class);
  private final Tracer tracer;
  private final OnlineRetrieverV2 retriever;
  private final FeatureSpecRetriever featureSpecRetriever;

  public OnlineServingServiceV2(
      OnlineRetrieverV2 retriever, Tracer tracer, FeatureSpecRetriever featureSpecRetriever) {
    this.retriever = retriever;
    this.tracer = tracer;
    this.featureSpecRetriever = featureSpecRetriever;
  }

  /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequestV2 request) {
    // Autofill default project if project is not specified
    String projectName = request.getProject();
    if (projectName.isEmpty()) {
      projectName = "default";
    }

    // Split all feature references into batch feature view and ODFV references.
    List<FeatureReferenceV2> allFeatureReferences = request.getFeaturesList();
    List<FeatureReferenceV2> featureReferences =
        allFeatureReferences.stream()
            .filter(r -> this.featureSpecRetriever.isBatchFeatureReference(r))
            .collect(Collectors.toList());
    List<FeatureReferenceV2> onDemandFeatureReferences =
        allFeatureReferences.stream()
            .filter(r -> this.featureSpecRetriever.isOnDemandFeatureReference(r))
            .collect(Collectors.toList());

    // Get the set of request data feature names from the ODFV references.
    // Also get the batch feature view references that the ODFVs require as inputs.
    Set<String> requestDataFeatureNames = new HashSet<String>();
    List<FeatureReferenceV2> onDemandFeatureInputs = new ArrayList<FeatureReferenceV2>();
    for (FeatureReferenceV2 featureReference : onDemandFeatureReferences) {
      OnDemandFeatureViewSpec onDemandFeatureViewSpec =
          this.featureSpecRetriever.getOnDemandFeatureViewSpec(projectName, featureReference);
      Map<String, OnDemandInput> inputs = onDemandFeatureViewSpec.getInputsMap();

      for (OnDemandInput input : inputs.values()) {
        OnDemandInput.InputCase inputCase = input.getInputCase();
        if (inputCase.equals(inputCase.REQUEST_DATA_SOURCE)) {
          DataSource requestDataSource = input.getRequestDataSource();
          RequestDataOptions requestDataOptions = requestDataSource.getRequestDataOptions();
          Set<String> requestDataNames = requestDataOptions.getSchemaMap().keySet();
          requestDataFeatureNames.addAll(requestDataNames);
        } else if (inputCase.equals(inputCase.FEATURE_VIEW)) {
          FeatureView featureView = input.getFeatureView();
          FeatureViewSpec featureViewSpec = featureView.getSpec();
          String featureViewName = featureViewSpec.getName();
          for (FeatureSpecV2 featureSpec : featureViewSpec.getFeaturesList()) {
            String featureName = featureSpec.getName();
            FeatureReferenceV2 onDemandFeatureInput =
                FeatureReferenceV2.newBuilder()
                    .setFeatureTable(featureViewName)
                    .setName(featureName)
                    .build();
            onDemandFeatureInputs.add(onDemandFeatureInput);
          }
        }
      }
    }

    // Add on demand feature inputs to list of feature references to retrieve.
    Set<FeatureReferenceV2> addedFeatureReferences = new HashSet<FeatureReferenceV2>();
    for (FeatureReferenceV2 onDemandFeatureInput : onDemandFeatureInputs) {
      if (!featureReferences.contains(onDemandFeatureInput)) {
        featureReferences.add(onDemandFeatureInput);
        addedFeatureReferences.add(onDemandFeatureInput);
      }
    }

    // Separate entity rows into entity data and request feature data.
    List<GetOnlineFeaturesRequestV2.EntityRow> entityRows =
        new ArrayList<GetOnlineFeaturesRequestV2.EntityRow>();
    Map<String, List<ValueProto.Value>> requestDataFeatures =
        new HashMap<String, List<ValueProto.Value>>();

    for (GetOnlineFeaturesRequestV2.EntityRow entityRow : request.getEntityRowsList()) {
      Map<String, ValueProto.Value> fieldsMap = new HashMap<String, ValueProto.Value>();

      for (Map.Entry<String, ValueProto.Value> entry : entityRow.getFieldsMap().entrySet()) {
        String key = entry.getKey();
        ValueProto.Value value = entry.getValue();

        if (requestDataFeatureNames.contains(key)) {
          if (!requestDataFeatures.containsKey(key)) {
            requestDataFeatures.put(key, new ArrayList<ValueProto.Value>());
          }
          requestDataFeatures.get(key).add(value);
        } else {
          fieldsMap.put(key, value);
        }
      }

      // Construct new entity row containing the extracted entity data, if necessary.
      if (fieldsMap.size() > 0) {
        GetOnlineFeaturesRequestV2.EntityRow newEntityRow =
            GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
                .setTimestamp(entityRow.getTimestamp())
                .putAllFields(fieldsMap)
                .build();
        entityRows.add(newEntityRow);
      }
    }
    // TODO: error checking on lengths of lists in entityRows and requestDataFeatures

    for (Map.Entry<String, List<ValueProto.Value>> entry : requestDataFeatures.entrySet()) {
      String key = entry.getKey();
      List<ValueProto.Value> values = entry.getValue();
    }

    // Extract values and statuses to be used later in constructing FieldValues for the response.
    // The online features retrieved will augment these two data structures.
    List<Map<String, ValueProto.Value>> values =
        entityRows.stream().map(r -> new HashMap<>(r.getFieldsMap())).collect(Collectors.toList());
    List<Map<String, GetOnlineFeaturesResponse.FieldStatus>> statuses =
        entityRows.stream()
            .map(
                r ->
                    r.getFieldsMap().entrySet().stream()
                        .map(entry -> Pair.of(entry.getKey(), getMetadata(entry.getValue(), false)))
                        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight)))
            .collect(Collectors.toList());

    String finalProjectName = projectName;
    Map<FeatureReferenceV2, Duration> featureMaxAges =
        featureReferences.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ref -> this.featureSpecRetriever.getMaxAge(finalProjectName, ref)));
    List<String> entityNames =
        featureReferences.stream()
            .map(ref -> this.featureSpecRetriever.getEntitiesList(finalProjectName, ref))
            .findFirst()
            .get();

    Map<FeatureReferenceV2, ValueProto.ValueType.Enum> featureValueTypes =
        featureReferences.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ref -> {
                      try {
                        return this.featureSpecRetriever
                            .getFeatureSpec(finalProjectName, ref)
                            .getValueType();
                      } catch (SpecRetrievalException e) {
                        return ValueProto.ValueType.Enum.INVALID;
                      }
                    }));

    Span storageRetrievalSpan = tracer.buildSpan("storageRetrieval").start();
    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.setTag("entities", entityRows.size());
      storageRetrievalSpan.setTag("features", featureReferences.size());
    }
    List<List<Feature>> entityRowsFeatures =
        retriever.getOnlineFeatures(projectName, entityRows, featureReferences, entityNames);
    if (storageRetrievalSpan != null) {
      storageRetrievalSpan.finish();
    }

    if (entityRowsFeatures.size() != entityRows.size()) {
      throw Status.INTERNAL
          .withDescription(
              "The no. of FeatureRow obtained from OnlineRetriever"
                  + "does not match no. of entityRow passed.")
          .asRuntimeException();
    }

    Span postProcessingSpan = tracer.buildSpan("postProcessing").start();

    for (int i = 0; i < entityRows.size(); i++) {
      GetOnlineFeaturesRequestV2.EntityRow entityRow = entityRows.get(i);
      List<Feature> curEntityRowFeatures = entityRowsFeatures.get(i);

      Map<FeatureReferenceV2, Feature> featureReferenceFeatureMap =
          getFeatureRefFeatureMap(curEntityRowFeatures);

      Map<String, ValueProto.Value> rowValues = values.get(i);
      Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);

      for (FeatureReferenceV2 featureReference : featureReferences) {
        if (featureReferenceFeatureMap.containsKey(featureReference)) {
          Feature feature = featureReferenceFeatureMap.get(featureReference);

          ValueProto.Value value =
              feature.getFeatureValue(featureValueTypes.get(feature.getFeatureReference()));

          Boolean isOutsideMaxAge =
              checkOutsideMaxAge(
                  feature, entityRow, featureMaxAges.get(feature.getFeatureReference()));

          if (!isOutsideMaxAge && value != null) {
            rowValues.put(FeatureV2.getFeatureStringRef(feature.getFeatureReference()), value);
          } else {
            rowValues.put(
                FeatureV2.getFeatureStringRef(feature.getFeatureReference()),
                ValueProto.Value.newBuilder().build());
          }

          rowStatuses.put(
              FeatureV2.getFeatureStringRef(feature.getFeatureReference()),
              getMetadata(value, isOutsideMaxAge));
        } else {
          rowValues.put(
              FeatureV2.getFeatureStringRef(featureReference),
              ValueProto.Value.newBuilder().build());

          rowStatuses.put(
              FeatureV2.getFeatureStringRef(featureReference), getMetadata(null, false));
        }
      }
      // Populate metrics/log request
      populateCountMetrics(rowStatuses, projectName);
    }

    if (postProcessingSpan != null) {
      postProcessingSpan.finish();
    }

    populateHistogramMetrics(entityRows, featureReferences, projectName);
    populateFeatureCountMetrics(featureReferences, projectName);

    // Finally, we handle ODFVs. For each ODFV ref, we send a TransformFeaturesRequest to the FTS.
    // The request should contain the entity data, the retrieved features, and the request data.
    // All of this data must be bundled together and serialized into the Arrow IPC format.
    if (onDemandFeatureReferences.size() > 0) {
      // TODO: avoid hardcoding FTS address
      final ManagedChannel channel =
          ManagedChannelBuilder.forTarget("localhost:6569").usePlaintext().build();
      TransformationServiceGrpc.TransformationServiceBlockingStub stub =
          TransformationServiceGrpc.newBlockingStub(channel);

      // Augment values, which contains the entity data and retrieved features, with the request
      // data.
      // Also augmented statuses.
      for (int i = 0; i < values.size(); i++) {
        Map<String, ValueProto.Value> rowValues = values.get(i);
        Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);

        for (Map.Entry<String, List<ValueProto.Value>> entry : requestDataFeatures.entrySet()) {
          String key = entry.getKey();
          List<ValueProto.Value> fieldValues = entry.getValue();
          rowValues.put(key, fieldValues.get(i));
          rowStatuses.put(key, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
        }
      }

      // Convert values into Arrow IPC format by construct a VectorSchemaRoot. Start by constructing
      // the named columns.
      Map<String, FieldVector> columnNameToColumn = new HashMap<String, FieldVector>();
      BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      Map<String, ValueProto.Value> firstAugmentedValue = values.get(0);
      for (Map.Entry<String, ValueProto.Value> entry : firstAugmentedValue.entrySet()) {
        // The Python FTS does not expect full feature names, so we extract the feature name.
        String fullFeatureName = entry.getKey();
        String columnName = FeatureV2.getFeatureName(fullFeatureName);
        ValueProto.Value value = entry.getValue();
        FieldVector column;
        ValueProto.Value.ValCase valCase = value.getValCase();
        // TODO: support all Feast types
        switch (valCase) {
          case INT32_VAL:
            column = new IntVector(columnName, allocator);
            break;
          case INT64_VAL:
            column = new BigIntVector(columnName, allocator);
            break;
          case DOUBLE_VAL:
            column = new Float8Vector(columnName, allocator);
            break;
          case FLOAT_VAL:
            column = new Float4Vector(columnName, allocator);
            break;
          default:
            column = null;
        }
        column.allocateNew();
        columnNameToColumn.put(columnName, column);
      }

      // Add in all the data, row by row.
      for (int i = 0; i < values.size(); i++) {
        Map<String, ValueProto.Value> augmentedValues = values.get(i);

        for (Map.Entry<String, ValueProto.Value> entry : augmentedValues.entrySet()) {
          String fullFeatureName = entry.getKey();
          String columnName = FeatureV2.getFeatureName(fullFeatureName);
          ValueProto.Value value = entry.getValue();

          FieldVector column = columnNameToColumn.get(columnName);
          ValueProto.Value.ValCase valCase = value.getValCase();
          // TODO: support all Feast types
          switch (valCase) {
            case INT32_VAL:
              ((IntVector) column).setSafe(i, value.getInt32Val());
              break;
            case INT64_VAL:
              ((BigIntVector) column).setSafe(i, value.getInt64Val());
              break;
            case DOUBLE_VAL:
              ((Float8Vector) column).setSafe(i, value.getDoubleVal());
              break;
            case FLOAT_VAL:
              ((Float4Vector) column).setSafe(i, value.getFloatVal());
              break;
            default:
              column = null;
          }
        }
      }

      // Construct the VectorSchemaRoot.
      List<Field> columnFields = new ArrayList<Field>();
      List<FieldVector> columns = new ArrayList<FieldVector>();
      for (FieldVector column : columnNameToColumn.values()) {
        column.setValueCount(values.size());
        columnFields.add(column.getField());
        columns.add(column);
      }
      VectorSchemaRoot schemaRoot = new VectorSchemaRoot(columnFields, columns);

      // Serialize into Arrow IPC format.
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ArrowFileWriter writer = new ArrowFileWriter(schemaRoot, null, Channels.newChannel(out));
      try {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        e.printStackTrace();
      }
      byte[] byteData = out.toByteArray();
      ByteString inputData = ByteString.copyFrom(byteData);
      ValueType transformationInput = ValueType.newBuilder().setArrowValue(inputData).build();

      // Send out requests to the FTS.
      Set<String> onDemandFeatureStringReferences =
          onDemandFeatureReferences.stream()
              .map(r -> FeatureV2.getFeatureStringRef(r))
              .collect(Collectors.toSet());
      for (FeatureReferenceV2 featureReference : onDemandFeatureReferences) {
        String onDemandFeatureViewName = featureReference.getFeatureTable();
        TransformFeaturesRequest transformFeaturesRequest =
            TransformFeaturesRequest.newBuilder()
                .setOnDemandFeatureViewName(onDemandFeatureViewName)
                .setProject(projectName)
                .setTransformationInput(transformationInput)
                .build();

        TransformFeaturesResponse transformFeaturesResponse =
            stub.transformFeatures(transformFeaturesRequest);

        // Add response data back into values. Also add statuses.
        try {
          ArrowFileReader reader =
              new ArrowFileReader(
                  new ByteArrayReadableSeekableByteChannel(
                      transformFeaturesResponse
                          .getTransformationOutput()
                          .getArrowValue()
                          .toByteArray()),
                  allocator);
          reader.loadNextBatch();
          VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();

          Schema responseSchema = readBatch.getSchema();
          List<Field> responseFields = responseSchema.getFields();
          for (Field field : responseFields) {
            String columnName = field.getName();
            String fullFeatureName = onDemandFeatureViewName + ":" + columnName;
            ArrowType columnType = field.getType();

            // The response will contain all features for the specified ODFV, so we
            // skip the features that were not requested.
            if (!onDemandFeatureStringReferences.contains(fullFeatureName)) {
              continue;
            }

            FieldVector fieldVector = readBatch.getVector(field);
            int valueCount = fieldVector.getValueCount();

            // TODO: support all Feast types
            if (columnType instanceof ArrowType.Int) {
              int bitWidth = ((ArrowType.Int) columnType).getBitWidth();
              if (bitWidth == 64) {
                // handle as int64
                for (int i = 0; i < valueCount; i++) {
                  long int64Value = ((BigIntVector) fieldVector).get(i);
                  Map<String, ValueProto.Value> rowValues = values.get(i);
                  Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);
                  ValueProto.Value value =
                      ValueProto.Value.newBuilder().setInt64Val(int64Value).build();
                  rowValues.put(fullFeatureName, value);
                  rowStatuses.put(fullFeatureName, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
                }
              } else if (bitWidth == 32) {
                // handle as int32
                for (int i = 0; i < valueCount; i++) {
                  int intValue = ((IntVector) fieldVector).get(i);
                  Map<String, ValueProto.Value> rowValues = values.get(i);
                  Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);
                  ValueProto.Value value =
                      ValueProto.Value.newBuilder().setInt32Val(intValue).build();
                  rowValues.put(fullFeatureName, value);
                  rowStatuses.put(fullFeatureName, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
                }
              }
            } else if (columnType instanceof ArrowType.FloatingPoint) {
              FloatingPointPrecision precision =
                  ((ArrowType.FloatingPoint) columnType).getPrecision();
              if (precision == FloatingPointPrecision.DOUBLE) {
                // handle as double
                for (int i = 0; i < valueCount; i++) {
                  double doubleValue = ((Float8Vector) fieldVector).get(i);
                  Map<String, ValueProto.Value> rowValues = values.get(i);
                  Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);
                  ValueProto.Value value =
                      ValueProto.Value.newBuilder().setDoubleVal(doubleValue).build();
                  rowValues.put(fullFeatureName, value);
                  rowStatuses.put(fullFeatureName, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
                }
              } else if (precision == FloatingPointPrecision.SINGLE) {
                // handle as float
                for (int i = 0; i < valueCount; i++) {
                  float floatValue = ((Float4Vector) fieldVector).get(i);
                  Map<String, ValueProto.Value> rowValues = values.get(i);
                  Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);
                  ValueProto.Value value =
                      ValueProto.Value.newBuilder().setFloatVal(floatValue).build();
                  rowValues.put(fullFeatureName, value);
                  rowStatuses.put(fullFeatureName, GetOnlineFeaturesResponse.FieldStatus.PRESENT);
                }
              }
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      channel.shutdownNow();

      // Remove all features that were added as inputs for ODFVs.
      Set<String> addedFeatureStringReferences =
          addedFeatureReferences.stream()
              .map(r -> FeatureV2.getFeatureStringRef(r))
              .collect(Collectors.toSet());
      for (int i = 0; i < values.size(); i++) {
        Map<String, ValueProto.Value> rowValues = values.get(i);
        Map<String, GetOnlineFeaturesResponse.FieldStatus> rowStatuses = statuses.get(i);
        List<String> keysToRemove =
            rowValues.keySet().stream()
                .filter(k -> addedFeatureStringReferences.contains(k))
                .collect(Collectors.toList());
        for (String key : keysToRemove) {
          rowValues.remove(key);
          rowStatuses.remove(key);
        }
      }
    }

    // Build response field values from entityValuesMap and entityStatusesMap
    // Response field values should be in the same order as the entityRows provided by the user.
    List<GetOnlineFeaturesResponse.FieldValues> fieldValuesList =
        IntStream.range(0, entityRows.size())
            .mapToObj(
                entityRowIdx ->
                    GetOnlineFeaturesResponse.FieldValues.newBuilder()
                        .putAllFields(values.get(entityRowIdx))
                        .putAllStatuses(statuses.get(entityRowIdx))
                        .build())
            .collect(Collectors.toList());

    return GetOnlineFeaturesResponse.newBuilder().addAllFieldValues(fieldValuesList).build();
  }

  private static Map<FeatureReferenceV2, Feature> getFeatureRefFeatureMap(List<Feature> features) {
    return features.stream()
        .collect(Collectors.toMap(Feature::getFeatureReference, Function.identity()));
  }

  /**
   * Generate Field level Status metadata for the given valueMap.
   *
   * @param value value to generate metadata for.
   * @param isOutsideMaxAge whether the given valueMap contains values with age outside
   *     FeatureTable's max age.
   * @return a 1:1 map keyed by field name containing field status metadata instead of values in the
   *     given valueMap.
   */
  private static GetOnlineFeaturesResponse.FieldStatus getMetadata(
      ValueProto.Value value, boolean isOutsideMaxAge) {

    if (value == null) {
      return GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND;
    } else if (isOutsideMaxAge) {
      return GetOnlineFeaturesResponse.FieldStatus.OUTSIDE_MAX_AGE;
    } else if (value.getValCase().equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
      return GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE;
    }
    return GetOnlineFeaturesResponse.FieldStatus.PRESENT;
  }

  /**
   * Determine if the feature data in the given feature row is outside maxAge. Data is outside
   * maxAge to be when the difference ingestion time set in feature row and the retrieval time set
   * in entity row exceeds FeatureTable max age.
   *
   * @param feature contains the ingestion timing and feature data.
   * @param entityRow contains the retrieval timing of when features are pulled.
   * @param maxAge feature's max age.
   */
  private static boolean checkOutsideMaxAge(
      Feature feature, GetOnlineFeaturesRequestV2.EntityRow entityRow, Duration maxAge) {

    if (maxAge.equals(Duration.getDefaultInstance())) { // max age is not set
      return false;
    }

    long givenTimestamp = entityRow.getTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - feature.getEventTimestamp().getSeconds();
    return timeDifference > maxAge.getSeconds();
  }

  /**
   * Populate histogram metrics that can be used for analysing online retrieval calls
   *
   * @param entityRows entity rows provided in request
   * @param featureReferences feature references provided in request
   * @param project project name provided in request
   */
  private void populateHistogramMetrics(
      List<GetOnlineFeaturesRequestV2.EntityRow> entityRows,
      List<FeatureReferenceV2> featureReferences,
      String project) {
    Metrics.requestEntityCountDistribution
        .labels(project)
        .observe(Double.valueOf(entityRows.size()));
    Metrics.requestFeatureCountDistribution
        .labels(project)
        .observe(Double.valueOf(featureReferences.size()));

    long countDistinctFeatureTables =
        featureReferences.stream()
            .map(featureReference -> getFeatureTableStringRef(project, featureReference))
            .distinct()
            .count();
    Metrics.requestFeatureTableCountDistribution
        .labels(project)
        .observe(Double.valueOf(countDistinctFeatureTables));
  }

  /**
   * Populate count metrics that can be used for analysing online retrieval calls
   *
   * @param statusMap Statuses of features which have been requested
   * @param project Project where request for features was called from
   */
  private void populateCountMetrics(
      Map<String, GetOnlineFeaturesResponse.FieldStatus> statusMap, String project) {
    statusMap.forEach(
        (featureRefString, status) -> {
          if (status == GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND) {
            Metrics.notFoundKeyCount.labels(project, featureRefString).inc();
          }
          if (status == GetOnlineFeaturesResponse.FieldStatus.OUTSIDE_MAX_AGE) {
            Metrics.staleKeyCount.labels(project, featureRefString).inc();
          }
        });
  }

  private void populateFeatureCountMetrics(
      List<FeatureReferenceV2> featureReferences, String project) {
    featureReferences.forEach(
        featureReference ->
            Metrics.requestFeatureCount
                .labels(project, FeatureV2.getFeatureStringRef(featureReference))
                .inc());
  }
}
