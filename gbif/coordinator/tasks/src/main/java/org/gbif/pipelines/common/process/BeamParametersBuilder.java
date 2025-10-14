package org.gbif.pipelines.common.process;

import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import java.util.*;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.ElasticsearchConfiguration;
import org.gbif.pipelines.common.configs.IndexConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.tasks.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.dwcdp.DwcDpConfiguration;
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

    private final List<String> simpleArgs = new ArrayList<>();

    public void addSingleArg(String... args) {
      simpleArgs.addAll(Arrays.asList(args));
    }

    public BeamParameters put(String key, Object value) {
      map.put(key, String.valueOf(value));
      return this;
    }

    public BeamParameters putCondition(boolean condition, String key, String value) {
      if (condition) {
        map.put(key, value);
      }
      return this;
    }

    public BeamParameters putRequireNonNull(String key, Object value) {
      return put(key, Objects.requireNonNull(value));
    }

    public BeamParameters putIfPresent(String key, Object value) {
      Optional.ofNullable(value).ifPresent(result -> put(key, result));
      return this;
    }

    public List<String> toList() {
      List<String> args = new ArrayList<>(simpleArgs);
      args.addAll(
          map.entrySet().stream()
              .map(es -> "--" + es.getKey() + "=" + es.getValue())
              .collect(Collectors.toList()));
      return args;
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

    arguments
        .putIfPresent("defaultDateFormat", defaultDateFormat)
        .putCondition(
            isValidator(message.getPipelineSteps(), config.validatorOnly),
            "useMetadataWsCalls",
            "false");

    if (config.skipGbifIds) {
      arguments
          .put("tripletValid", "false")
          .put("occurrenceIdValid", "false")
          .put("useExtendedRecordId", "true");
    } else {
      Optional.ofNullable(message.getValidationResult())
          .ifPresent(
              vr ->
                  arguments
                      .put("tripletValid", String.valueOf(vr.isTripletValid()))
                      .put("occurrenceIdValid", String.valueOf(vr.isOccurrenceIdValid())));

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

    return IndexingCommon.builder()
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
        .create()
        // Extra
        .putIfPresent("backPressure", config.backPressure)
        .putCondition(config.esGeneratedIds, "esDocumentId", "");
  }

  public static BeamParameters dwcDpIndexing(
      DwcDpConfiguration config,
      DwcDpDownloadFinishedMessage message,
      IndexSettings indexSettings) {

    return IndexingCommon.builder()
        .datasetUuid(message.getDatasetUuid())
        .attempt(message.getAttempt())
        .stepConfig(config.stepConfig)
        .esConfig(config.esConfig)
        .indexConfig(config.indexConfig)
        .metaFileName("")
        .pipelinesConfigPath("")
        .esIndexName(indexSettings.getIndexName())
        .esShardsNumber(indexSettings.getNumberOfShards())
        .build()
        .create();
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

    return BeamParameters.create()
        .putRequireNonNull("datasetId", message.getDatasetUuid())
        .put("attempt", message.getAttempt())
        .put("runner", "SparkRunner")
        .putRequireNonNull("metaFileName", config.metaFileName)
        .putRequireNonNull("inputPath", config.stepConfig.repositoryPath)
        .putRequireNonNull("targetPath", config.repositoryTargetPath)
        .putRequireNonNull("hdfsSiteConfig", config.stepConfig.hdfsSiteConfig)
        .putRequireNonNull("coreSiteConfig", config.stepConfig.coreSiteConfig)
        .putRequireNonNull("properties", config.pipelinesConfig)
        .put("numberOfShards", numberOfShards)
        .putRequireNonNull("interpretationTypes", String.join(",", message.getInterpretTypes()))
        .put("experiments", "use_deprecated_read")
        .putCondition(config.recordType == RecordType.EVENT, "coreRecordType", "EVENT");
  }

  public static BeamParameters eventInterpretation(
      EventsInterpretationConfiguration config,
      PipelinesEventsMessage message,
      String inputPath,
      int numberOfShards) {

    return InterpretationCommon.builder()
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
        .create()
        // Extra
        .put("dwcCore", "Event");
  }

  public static BeamParameters eventIndexing(
      EventsIndexingConfiguration config,
      PipelinesEventsInterpretedMessage message,
      IndexSettings indexSettings) {

    return IndexingCommon.builder()
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
        .create()
        // Extra
        .put("datasetType", "SAMPLING_EVENT")
        .put("dwcCore", "Event")
        .putCondition(config.esGeneratedIds, "esDocumentId", "")
        .putCondition(!config.esGeneratedIds, "esDocumentId", "internalId");
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
            vr ->
                arguments
                    .put("tripletValid", String.valueOf(vr.isTripletValid()))
                    .put("occurrenceIdValid", String.valueOf(vr.isOccurrenceIdValid())));

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
      return BeamParameters.create()
          .putRequireNonNull("datasetId", datasetUuid)
          .put("attempt", attempt)
          .putRequireNonNull("interpretationTypes", String.join(",", interpretTypes))
          .put("runner", "SparkRunner")
          .putRequireNonNull("targetPath", stepConfig.repositoryPath)
          .putRequireNonNull("metaFileName", metaFileName)
          .putRequireNonNull("inputPath", inputPath)
          .putRequireNonNull("avroCompressionType", avroConfig.compressionType)
          .put("avroSyncInterval", avroConfig.syncInterval)
          .putRequireNonNull("hdfsSiteConfig", stepConfig.hdfsSiteConfig)
          .putRequireNonNull("coreSiteConfig", stepConfig.coreSiteConfig)
          .putRequireNonNull("properties", pipelinesConfigPath)
          .put("numberOfShards", numberOfShards)
          .put("experiments", "use_deprecated_read");
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
      return BeamParameters.create()
          .putRequireNonNull("datasetId", datasetUuid)
          .put("attempt", attempt)
          .put("runner", "SparkRunner")
          .putRequireNonNull("inputPath", stepConfig.repositoryPath)
          .putRequireNonNull("targetPath", stepConfig.repositoryPath)
          .putRequireNonNull("metaFileName", metaFileName)
          .putRequireNonNull("hdfsSiteConfig", stepConfig.hdfsSiteConfig)
          .putRequireNonNull("coreSiteConfig", stepConfig.coreSiteConfig)
          .putRequireNonNull("esHosts", String.join(",", esConfig.hosts))
          .putRequireNonNull("properties", pipelinesConfigPath)
          .putRequireNonNull("esIndexName", esIndexName)
          .put("experiments", "use_deprecated_read")
          .putIfPresent("esAlias", indexConfig.occurrenceAlias)
          .putIfPresent("esMaxBatchSizeBytes", esConfig.maxBatchSizeBytes)
          .putIfPresent("esMaxBatchSize", esConfig.maxBatchSize)
          .putIfPresent("esSchemaPath", esConfig.schemaPath)
          .putIfPresent("indexRefreshInterval", indexConfig.refreshInterval)
          .putIfPresent("indexNumberShards", esShardsNumber)
          .putIfPresent("indexNumberReplicas", indexConfig.numberReplicas)
          .putIfPresent("indexMappingTotalFieldsLimit", indexConfig.indexMappingTotalFieldsLimit);
    }
  }
}
