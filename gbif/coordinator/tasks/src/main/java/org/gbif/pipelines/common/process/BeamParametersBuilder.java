package org.gbif.pipelines.common.process;

import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.ElasticsearchConfiguration;
import org.gbif.pipelines.common.configs.IndexConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.tasks.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.events.indexing.EventsIndexingConfiguration;
import org.gbif.pipelines.tasks.events.interpretation.EventsInterpretationConfiguration;
import org.gbif.pipelines.tasks.occurrences.identifier.IdentifierConfiguration;
import org.gbif.pipelines.tasks.occurrences.indexing.IndexingConfiguration;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;
import org.gbif.pipelines.tasks.verbatims.fragmenter.FragmenterConfiguration;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BeamParametersBuilder {

  @NoArgsConstructor(staticName = "create")
  public static class BeamParameters {
    private final Map<String, String> map = new HashMap<>();

    public void put(String key, String value) {
      map.put(key, value);
    }

    public List<String> toList() {
      return map.entrySet().stream()
          .map(es -> "--" + es.getKey() + "=" + es.getValue())
          .collect(Collectors.toList());
    }

    public String[] toArray() {
      return toList().toArray(String[]::new);
    }
  }

  public static BeamParameters occurrenceInterpretation(
      InterpreterConfiguration config,
      PipelinesVerbatimMessage message,
      String inputPath,
      String defaultDateFormat,
      int numberOfShards) {

    BeamParameters arguments =
        InterpretationCommon.builder()
            .datasetUuid(message.getDatasetUuid())
            .attempt(message.getAttempt())
            .interpretTypes(message.getInterpretTypes())
            .stepConfig(config.stepConfig)
            .avroConfig(config.avroConfig)
            .pipelinesConfigPath(config.pipelinesConfig)
            .metaFileName(config.metaFileName)
            .inputPath(inputPath)
            .numberOfShards(numberOfShards)
            .build()
            .create();

    Optional.ofNullable(defaultDateFormat).ifPresent(x -> arguments.put("defaultDateFormat", x));

    if (isValidator(message.getPipelineSteps(), config.validatorOnly)) {
      arguments.put("useMetadataWsCalls", "false");
    }

    if (config.skipGbifIds) {
      arguments.put("tripletValid", "false");
      arguments.put("occurrenceIdValid", "false");
      arguments.put("useExtendedRecordId", "true");
    } else {
      Optional.ofNullable(message.getValidationResult())
          .ifPresent(
              vr -> {
                arguments.put("tripletValid", String.valueOf(vr.isTripletValid()));
                arguments.put("occurrenceIdValid", String.valueOf(vr.isOccurrenceIdValid()));
              });

      Optional.ofNullable(message.getValidationResult())
          .flatMap(vr -> Optional.ofNullable(vr.isUseExtendedRecordId()))
          .map(String::valueOf)
          .ifPresent(x -> arguments.put("useExtendedRecordId", x));
    }

    return arguments;
  }

  public static BeamParameters occurrenceIndexing(
      IndexingConfiguration config,
      PipelinesInterpretedMessage message,
      IndexSettings indexSettings) {

    BeamParameters arguments =
        IndexingCommon.builder()
            .datasetUuid(message.getDatasetUuid())
            .attempt(message.getAttempt())
            .stepConfig(config.stepConfig)
            .esConfig(config.esConfig)
            .indexConfig(config.indexConfig)
            .metaFileName(config.metaFileName)
            .pipelinesConfigPath(config.pipelinesConfig)
            .esIndexName(indexSettings.getIndexName())
            .esShardsNumber(indexSettings.getNumberOfShards())
            .build()
            .create();

    Optional.ofNullable(config.backPressure)
        .map(String::valueOf)
        .ifPresent(x -> arguments.put("backPressure", x));

    if (config.esGeneratedIds) {
      arguments.put("esDocumentId", "");
    }
    return arguments;
  }

  public static BeamParameters occurrenceIdentifier(
      IdentifierConfiguration config,
      PipelinesVerbatimMessage message,
      String inputPath,
      int numberOfShards) {
    BeamParameters arguments =
        InterpretationCommon.builder()
            .datasetUuid(message.getDatasetUuid())
            .attempt(message.getAttempt())
            .interpretTypes(message.getInterpretTypes())
            .stepConfig(config.stepConfig)
            .avroConfig(config.avroConfig)
            .pipelinesConfigPath(config.pipelinesConfig)
            .metaFileName(config.metaFileName)
            .inputPath(inputPath)
            .numberOfShards(numberOfShards)
            .build()
            .create();

    Optional.ofNullable(message.getValidationResult())
        .ifPresent(
            vr -> {
              arguments.put("tripletValid", String.valueOf(vr.isTripletValid()));
              arguments.put("occurrenceIdValid", String.valueOf(vr.isOccurrenceIdValid()));
            });

    Optional.ofNullable(message.getValidationResult())
        .flatMap(vr -> Optional.ofNullable(vr.isUseExtendedRecordId()))
        .map(String::valueOf)
        .ifPresent(x -> arguments.put("useExtendedRecordId", x));
    return arguments;
  }

  public static BeamParameters occurrenceHdfsView(
      HdfsViewConfiguration config, PipelinesInterpretationMessage message, int numberOfShards) {

    BeamParameters arguments = BeamParameters.create();
    // Common properties
    arguments.put("datasetId", Objects.requireNonNull(message.getDatasetUuid()).toString());
    arguments.put("attempt", String.valueOf(message.getAttempt()));
    arguments.put("runner", "SparkRunner");
    arguments.put("metaFileName", Objects.requireNonNull(config.metaFileName));
    arguments.put("inputPath", Objects.requireNonNull(config.stepConfig.repositoryPath));
    arguments.put("targetPath", Objects.requireNonNull(config.repositoryTargetPath));
    arguments.put("hdfsSiteConfig", Objects.requireNonNull(config.stepConfig.hdfsSiteConfig));
    arguments.put("coreSiteConfig", Objects.requireNonNull(config.stepConfig.coreSiteConfig));
    arguments.put("properties", Objects.requireNonNull(config.pipelinesConfig));
    arguments.put("numberOfShards", String.valueOf(numberOfShards));
    arguments.put(
        "interpretationTypes",
        Objects.requireNonNull(String.join(",", message.getInterpretTypes())));
    arguments.put("experiments", "use_deprecated_read");

    if (config.recordType == RecordType.EVENT) {
      arguments.put("coreRecordType", "EVENT");
    }
    return arguments;
  }

  public static BeamParameters eventInterpretation(
      EventsInterpretationConfiguration config,
      PipelinesEventsMessage message,
      String inputPath,
      int numberOfShards) {

    BeamParameters arguments =
        InterpretationCommon.builder()
            .datasetUuid(message.getDatasetUuid())
            .attempt(message.getAttempt())
            .interpretTypes(message.getInterpretTypes())
            .stepConfig(config.stepConfig)
            .avroConfig(config.avroConfig)
            .pipelinesConfigPath(config.pipelinesConfig)
            .metaFileName(config.metaFileName)
            .inputPath(inputPath)
            .numberOfShards(numberOfShards)
            .build()
            .create();

    arguments.put("dwcCore", "Event");

    return arguments;
  }

  public static BeamParameters eventIndexing(
      EventsIndexingConfiguration config,
      PipelinesEventsInterpretedMessage message,
      IndexSettings indexSettings) {

    BeamParameters arguments =
        IndexingCommon.builder()
            .datasetUuid(message.getDatasetUuid())
            .attempt(message.getAttempt())
            .stepConfig(config.stepConfig)
            .esConfig(config.esConfig)
            .indexConfig(config.indexConfig)
            .metaFileName(config.metaFileName)
            .pipelinesConfigPath(config.pipelinesConfig)
            .esIndexName(indexSettings.getIndexName())
            .esShardsNumber(indexSettings.getNumberOfShards())
            .build()
            .create();

    arguments.put("datasetType", "SAMPLING_EVENT");
    arguments.put("dwcCore", "Event");

    if (config.esGeneratedIds) {
      arguments.put("esDocumentId", "");
    } else {
      arguments.put("esDocumentId", "internalId");
    }

    return arguments;
  }

  public static BeamParameters verbatimFragmenter(
      FragmenterConfiguration config, PipelinesInterpretedMessage message) {

    BeamParameters arguments =
        InterpretationCommon.builder()
            .datasetUuid(message.getDatasetUuid())
            .attempt(message.getAttempt())
            .interpretTypes(message.getInterpretTypes())
            .stepConfig(config.stepConfig)
            .pipelinesConfigPath(config.pipelinesConfig)
            .metaFileName(config.metaFileName)
            .inputPath(config.stepConfig.repositoryPath)
            .avroConfig(config.avroConfig)
            .build()
            .create();

    arguments.put("generateIds", "false");

    Optional.ofNullable(message.getValidationResult())
        .ifPresent(
            vr -> {
              arguments.put("tripletValid", String.valueOf(vr.isTripletValid()));
              arguments.put("occurrenceIdValid", String.valueOf(vr.isOccurrenceIdValid()));
            });

    return arguments;
  }

  @Builder
  private static class InterpretationCommon {

    private final UUID datasetUuid;
    private final Integer attempt;
    private final Set<String> interpretTypes;
    private final StepConfiguration stepConfig;
    private final AvroWriteConfiguration avroConfig;
    private final String pipelinesConfigPath;
    private final String metaFileName;
    private final String inputPath;
    private final int numberOfShards;

    private BeamParameters create() {

      BeamParameters arguments = BeamParameters.create();

      String interpretationTypes = String.join(",", interpretTypes);

      arguments.put("datasetId", Objects.requireNonNull(datasetUuid).toString());
      arguments.put("attempt", String.valueOf(attempt));
      arguments.put("interpretationTypes", Objects.requireNonNull(interpretationTypes));
      arguments.put("runner", "SparkRunner");
      arguments.put("targetPath", Objects.requireNonNull(stepConfig.repositoryPath));
      arguments.put("metaFileName", Objects.requireNonNull(metaFileName));
      arguments.put("inputPath", Objects.requireNonNull(inputPath));
      arguments.put("avroCompressionType", Objects.requireNonNull(avroConfig.compressionType));
      arguments.put("avroSyncInterval", String.valueOf(avroConfig.syncInterval));
      arguments.put("hdfsSiteConfig", Objects.requireNonNull(stepConfig.hdfsSiteConfig));
      arguments.put("coreSiteConfig", Objects.requireNonNull(stepConfig.coreSiteConfig));
      arguments.put("properties", Objects.requireNonNull(pipelinesConfigPath));
      arguments.put("numberOfShards", String.valueOf(numberOfShards));
      arguments.put("experiments", "use_deprecated_read");

      return arguments;
    }
  }

  @Builder
  private static class IndexingCommon {

    private final UUID datasetUuid;
    private final Integer attempt;
    private final StepConfiguration stepConfig;
    private final ElasticsearchConfiguration esConfig;
    private final IndexConfiguration indexConfig;
    private final String pipelinesConfigPath;
    private final String metaFileName;
    private final String esIndexName;
    private final Integer esShardsNumber;

    private BeamParameters create() {

      BeamParameters arguments = BeamParameters.create();

      String esHosts = String.join(",", esConfig.hosts);

      // Common properties
      arguments.put("datasetId", Objects.requireNonNull(datasetUuid).toString());
      arguments.put("attempt", String.valueOf(attempt));
      arguments.put("runner", "SparkRunner");
      arguments.put("inputPath", Objects.requireNonNull(stepConfig.repositoryPath));
      arguments.put("targetPath", Objects.requireNonNull(stepConfig.repositoryPath));
      arguments.put("metaFileName", Objects.requireNonNull(metaFileName));
      arguments.put("hdfsSiteConfig", Objects.requireNonNull(stepConfig.hdfsSiteConfig));
      arguments.put("coreSiteConfig", Objects.requireNonNull(stepConfig.coreSiteConfig));
      arguments.put("esHosts", Objects.requireNonNull(esHosts));
      arguments.put("properties", Objects.requireNonNull(pipelinesConfigPath));
      arguments.put("esIndexName", Objects.requireNonNull(esIndexName));
      arguments.put("experiments", "use_deprecated_read");

      Optional.ofNullable(indexConfig.occurrenceAlias).ifPresent(x -> arguments.put("esAlias", x));
      Optional.ofNullable(esConfig.maxBatchSizeBytes)
          .map(String::valueOf)
          .ifPresent(x -> arguments.put("esMaxBatchSizeBytes", x));
      Optional.ofNullable(esConfig.maxBatchSize)
          .map(String::valueOf)
          .ifPresent(x -> arguments.put("esMaxBatchSize", x));
      Optional.ofNullable(esConfig.schemaPath).ifPresent(x -> arguments.put("esSchemaPath", x));
      Optional.ofNullable(indexConfig.refreshInterval)
          .ifPresent(x -> arguments.put("indexRefreshInterval", x));
      Optional.ofNullable(esShardsNumber)
          .map(String::valueOf)
          .ifPresent(x -> arguments.put("indexNumberShards", x));
      Optional.ofNullable(indexConfig.numberReplicas)
          .map(String::valueOf)
          .ifPresent(x -> arguments.put("indexNumberReplicas", x));

      return arguments;
    }
  }
}
