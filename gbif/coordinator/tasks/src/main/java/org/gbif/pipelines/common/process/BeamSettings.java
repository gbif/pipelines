package org.gbif.pipelines.common.process;

import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.ElasticsearchConfiguration;
import org.gbif.pipelines.common.configs.IndexConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.tasks.events.indexing.EventsIndexingConfiguration;
import org.gbif.pipelines.tasks.events.interpretation.EventsInterpretationConfiguration;
import org.gbif.pipelines.tasks.occurrences.identifier.IdentifierConfiguration;
import org.gbif.pipelines.tasks.occurrences.indexing.IndexingConfiguration;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BeamSettings {

  public static Consumer<StringJoiner> occurrenceInterpretation(
      InterpreterConfiguration config,
      PipelinesVerbatimMessage message,
      String inputPath,
      String defaultDateFormat) {
    return command -> {
      InterpretationCommon.builder()
          .command(command)
          .datasetUuid(message.getDatasetUuid())
          .attempt(message.getAttempt())
          .endpointType(message.getEndpointType())
          .interpretTypes(message.getInterpretTypes())
          .stepConfig(config.stepConfig)
          .avroConfig(config.avroConfig)
          .pipelinesConfigPath(config.pipelinesConfig)
          .metaFileName(config.metaFileName)
          .inputPath(inputPath)
          .useBeamDeprecatedRead(config.useBeamDeprecatedRead)
          .build()
          .addToStringBuilder();

      Optional.ofNullable(defaultDateFormat)
          .ifPresent(x -> command.add("--defaultDateFormat=" + x));

      if (isValidator(message.getPipelineSteps(), config.validatorOnly)) {
        command.add("--useMetadataWsCalls=false");
      }

      if (config.skipGbifIds) {
        command
            .add("--tripletValid=false")
            .add("--occurrenceIdValid=false")
            .add("--useExtendedRecordId=true");
      } else {
        Optional.ofNullable(message.getValidationResult())
            .ifPresent(
                vr ->
                    command
                        .add("--tripletValid=" + vr.isTripletValid())
                        .add("--occurrenceIdValid=" + vr.isOccurrenceIdValid()));

        Optional.ofNullable(message.getValidationResult())
            .flatMap(vr -> Optional.ofNullable(vr.isUseExtendedRecordId()))
            .ifPresent(x -> command.add("--useExtendedRecordId=" + x));
      }
    };
  }

  public static Consumer<StringJoiner> occurreceIndexing(
      IndexingConfiguration config,
      PipelinesInterpretedMessage message,
      IndexSettings indexSettings) {
    return command -> {
      IndexingCommon.builder()
          .command(command)
          .datasetUuid(message.getDatasetUuid())
          .attempt(message.getAttempt())
          .stepConfig(config.stepConfig)
          .esConfig(config.esConfig)
          .indexConfig(config.indexConfig)
          .metaFileName(config.metaFileName)
          .pipelinesConfigPath(config.pipelinesConfig)
          .esIndexName(indexSettings.getIndexName())
          .esShardsNumber(indexSettings.getNumberOfShards())
          .useBeamDeprecatedRead(config.useBeamDeprecatedRead)
          .build()
          .addToStringBuilder();

      Optional.ofNullable(config.backPressure).ifPresent(x -> command.add("--backPressure=" + x));

      if (config.esGeneratedIds) {
        command.add("--esDocumentId=");
      }
    };
  }

  public static Consumer<StringJoiner> occurrenceIdentifier(
      IdentifierConfiguration config, PipelinesVerbatimMessage message, String inputPath) {
    return command -> {
      InterpretationCommon.builder()
          .command(command)
          .datasetUuid(message.getDatasetUuid())
          .attempt(message.getAttempt())
          .endpointType(message.getEndpointType())
          .interpretTypes(message.getInterpretTypes())
          .stepConfig(config.stepConfig)
          .avroConfig(config.avroConfig)
          .pipelinesConfigPath(config.pipelinesConfig)
          .metaFileName(config.metaFileName)
          .inputPath(inputPath)
          .useBeamDeprecatedRead(config.useBeamDeprecatedRead)
          .build()
          .addToStringBuilder();

      Optional.ofNullable(message.getValidationResult())
          .ifPresent(
              vr ->
                  command
                      .add("--tripletValid=" + vr.isTripletValid())
                      .add("--occurrenceIdValid=" + vr.isOccurrenceIdValid()));

      Optional.ofNullable(message.getValidationResult())
          .flatMap(vr -> Optional.ofNullable(vr.isUseExtendedRecordId()))
          .ifPresent(x -> command.add("--useExtendedRecordId=" + x));
    };
  }

  public static Consumer<StringJoiner> occurrenceHdfsView(
      HdfsViewConfiguration config, PipelinesInterpretationMessage message, int numberOfShards) {
    return command -> {
      // Common properties
      command
          .add("--datasetId=" + Objects.requireNonNull(message.getDatasetUuid()))
          .add("--attempt=" + message.getAttempt())
          .add("--runner=SparkRunner")
          .add("--metaFileName=" + Objects.requireNonNull(config.metaFileName))
          .add("--inputPath=" + Objects.requireNonNull(config.stepConfig.repositoryPath))
          .add("--targetPath=" + Objects.requireNonNull(config.repositoryTargetPath))
          .add("--hdfsSiteConfig=" + Objects.requireNonNull(config.stepConfig.hdfsSiteConfig))
          .add("--coreSiteConfig=" + Objects.requireNonNull(config.stepConfig.coreSiteConfig))
          .add("--properties=" + Objects.requireNonNull(config.pipelinesConfig))
          .add("--numberOfShards=" + numberOfShards)
          .add(
              "--interpretationTypes="
                  + Objects.requireNonNull(String.join(",", message.getInterpretTypes())));

      if (config.useBeamDeprecatedRead) {
        command.add("--experiments=use_deprecated_read");
      }

      if (config.recordType == RecordType.EVENT) {
        command.add("--coreRecordType=EVENT");
      }
    };
  }

  public static Consumer<StringJoiner> eventInterpretation(
      EventsInterpretationConfiguration config, PipelinesEventsMessage message, String inputPath) {
    return command -> {
      InterpretationCommon.builder()
          .command(command)
          .datasetUuid(message.getDatasetUuid())
          .attempt(message.getAttempt())
          .endpointType(message.getEndpointType())
          .interpretTypes(message.getInterpretTypes())
          .stepConfig(config.stepConfig)
          .avroConfig(config.avroConfig)
          .pipelinesConfigPath(config.pipelinesConfig)
          .metaFileName(config.metaFileName)
          .inputPath(inputPath)
          .useBeamDeprecatedRead(config.useBeamDeprecatedRead)
          .build()
          .addToStringBuilder();

      command.add("--dwcCore=Event");
    };
  }

  public static Consumer<StringJoiner> eventIndexing(
      EventsIndexingConfiguration config,
      PipelinesEventsInterpretedMessage message,
      IndexSettings indexSettings) {
    return command -> {
      IndexingCommon.builder()
          .command(command)
          .datasetUuid(message.getDatasetUuid())
          .attempt(message.getAttempt())
          .stepConfig(config.stepConfig)
          .esConfig(config.esConfig)
          .indexConfig(config.indexConfig)
          .metaFileName(config.metaFileName)
          .pipelinesConfigPath(config.pipelinesConfig)
          .esIndexName(indexSettings.getIndexName())
          .esShardsNumber(indexSettings.getNumberOfShards())
          .useBeamDeprecatedRead(config.useBeamDeprecatedRead)
          .build()
          .addToStringBuilder();

      command.add("--datasetType=SAMPLING_EVENT").add("--dwcCore=Event");

      if (config.esGeneratedIds) {
        command.add("--esDocumentId=");
      } else {
        command.add("--esDocumentId=internalId");
      }
    };
  }

  @Builder
  private static class InterpretationCommon {

    private final StringJoiner command;
    private final UUID datasetUuid;
    private final Integer attempt;
    private final EndpointType endpointType;
    private final Set<String> interpretTypes;
    private final StepConfiguration stepConfig;
    private final AvroWriteConfiguration avroConfig;
    private final String pipelinesConfigPath;
    private final String metaFileName;
    private final String inputPath;
    private final boolean useBeamDeprecatedRead;

    private void addToStringBuilder() {
      String interpretationTypes = String.join(",", interpretTypes);

      command
          .add("--datasetId=" + Objects.requireNonNull(datasetUuid))
          .add("--attempt=" + attempt)
          .add("--interpretationTypes=" + Objects.requireNonNull(interpretationTypes))
          .add("--runner=SparkRunner")
          .add("--targetPath=" + Objects.requireNonNull(stepConfig.repositoryPath))
          .add("--metaFileName=" + Objects.requireNonNull(metaFileName))
          .add("--inputPath=" + Objects.requireNonNull(inputPath))
          .add("--avroCompressionType=" + Objects.requireNonNull(avroConfig.compressionType))
          .add("--avroSyncInterval=" + avroConfig.syncInterval)
          .add("--hdfsSiteConfig=" + Objects.requireNonNull(stepConfig.hdfsSiteConfig))
          .add("--coreSiteConfig=" + Objects.requireNonNull(stepConfig.coreSiteConfig))
          .add("--properties=" + Objects.requireNonNull(pipelinesConfigPath))
          .add("--endPointType=" + Objects.requireNonNull(endpointType));

      if (useBeamDeprecatedRead) {
        command.add("--experiments=use_deprecated_read");
      }
    }
  }

  @Builder
  private static class IndexingCommon {

    private final StringJoiner command;
    private final UUID datasetUuid;
    private final Integer attempt;
    private final StepConfiguration stepConfig;
    private final ElasticsearchConfiguration esConfig;
    private final IndexConfiguration indexConfig;
    private final String pipelinesConfigPath;
    private final String metaFileName;
    private final String esIndexName;
    private final Integer esShardsNumber;
    private final boolean useBeamDeprecatedRead;

    private void addToStringBuilder() {
      String esHosts = String.join(",", esConfig.hosts);

      // Common properties
      command
          .add("--datasetId=" + Objects.requireNonNull(datasetUuid))
          .add("--attempt=" + attempt)
          .add("--runner=SparkRunner")
          .add("--inputPath=" + Objects.requireNonNull(stepConfig.repositoryPath))
          .add("--targetPath=" + Objects.requireNonNull(stepConfig.repositoryPath))
          .add("--metaFileName=" + Objects.requireNonNull(metaFileName))
          .add("--hdfsSiteConfig=" + Objects.requireNonNull(stepConfig.hdfsSiteConfig))
          .add("--coreSiteConfig=" + Objects.requireNonNull(stepConfig.coreSiteConfig))
          .add("--esHosts=" + Objects.requireNonNull(esHosts))
          .add("--properties=" + Objects.requireNonNull(pipelinesConfigPath))
          .add("--esIndexName=" + Objects.requireNonNull(esIndexName));

      Optional.ofNullable(indexConfig.occurrenceAlias)
          .ifPresent(x -> command.add("--esAlias=" + x));
      Optional.ofNullable(esConfig.maxBatchSizeBytes)
          .ifPresent(x -> command.add("--esMaxBatchSizeBytes=" + x));
      Optional.ofNullable(esConfig.maxBatchSize)
          .ifPresent(x -> command.add("--esMaxBatchSize=" + x));
      Optional.ofNullable(esConfig.schemaPath).ifPresent(x -> command.add("--esSchemaPath=" + x));
      Optional.ofNullable(indexConfig.refreshInterval)
          .ifPresent(x -> command.add("--indexRefreshInterval=" + x));
      Optional.ofNullable(esShardsNumber).ifPresent(x -> command.add("--indexNumberShards=" + x));
      Optional.ofNullable(indexConfig.numberReplicas)
          .ifPresent(x -> command.add("--indexNumberReplicas=" + x));

      if (useBeamDeprecatedRead) {
        command.add("--experiments=use_deprecated_read");
      }
    }
  }
}
