/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.tasks.verbatims.dwca.validator;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesValidatorDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.dwc.Archive;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.process.RecordCountReader;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.getAllValidatorInterpretationAsString;
import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

/** Callback which is called when the {@link PipelinesValidatorDwcaMessage} is received. */
@Slf4j
@Builder
public class DwcaToAvroValidatorCallback extends AbstractMessageCallback<PipelinesValidatorDwcaMessage>
    implements StepHandler<PipelinesValidatorDwcaMessage, PipelinesVerbatimMessage> {

  private final DwcaToAvroValidatorConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
  private final ValidationWsClient validationClient;

  @Override
  public void handleMessage(PipelinesValidatorDwcaMessage message) {
    PipelinesCallback.<PipelinesValidatorDwcaMessage, PipelinesVerbatimMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .validationClient(validationClient)
        .config(config)
        .stepType(StepType.VALIDATOR_DWCA_TO_VERBATIM)
        .isValidator(true)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    return PipelinesValidatorDwcaMessage.ROUTING_KEY;
  }

  /** Only correct messages can be handled, by now is only OCCURRENCE type messages */
  @Override
  public boolean isMessageCorrect(PipelinesValidatorDwcaMessage message) {
    boolean isMessageValid =
        message.getDatasetType() != null && message.getValidationReport().isValid();
    if (!isMessageValid) {
      log.info(
          "Skipping the task, because of the message is not valid, data type is {}, validation report is valid = {}, reason = {}",
          message.getDatasetType(),
          message.getValidationReport().isValid(),
          message.getValidationReport().getInvalidationReason());
    }
    boolean isReportValid = isReportValid(message);
    if (!isReportValid) {
      log.info(
          "Skipping the task, because of the validation report is missed or there are no records");
    }
    return isMessageValid && isReportValid;
  }

  private boolean isReportValid(PipelinesValidatorDwcaMessage message) {
    DwcaValidationReport report = message.getValidationReport();

    boolean isValidOccurrenceReport =
        report.getOccurrenceReport() != null
            && (report.getOccurrenceReport().getUniqueOccurrenceIds() > 0
            || report.getOccurrenceReport().getUniqueTriplets() > 0);

    boolean isValidGenericReport =
        message.getValidationReport().getGenericReport() != null
            && message.getValidationReport().getGenericReport().getCheckedRecords() > 0
            && message.getDatasetType() != DatasetType.CHECKLIST;

    if (!config.stepConfig.eventsEnabled
        && message.getDatasetType() == DatasetType.SAMPLING_EVENT) {
      isValidGenericReport = false;
    }

    return isValidOccurrenceReport || isValidGenericReport;
  }

  /** Main message processing logic converts a DwCA archive to an avro file. */
  @Override
  public Runnable createRunnable(PipelinesValidatorDwcaMessage message) {
    return () -> {
      UUID datasetId = message.getDatasetUuid();
      String attempt = String.valueOf(message.getAttempt());

      // Calculates and checks the existence of DwC Archive
      Path inputPath = buildDwcaInputPath(config.archiveRepository, datasetId);

      // Calculates the export path of avro as an extended record
      org.apache.hadoop.fs.Path outputPath =
          HdfsUtils.buildOutputPath(
              config.stepConfig.repositoryPath, datasetId.toString(), attempt, config.fileName);

      // Calculates the metadata path, the YAML file with the total number of converted records
      org.apache.hadoop.fs.Path metaPath =
          HdfsUtils.buildOutputPath(
              config.stepConfig.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      HdfsConfigs hdfsConfigs =
          HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
      // Run the main conversion process
      DwcaToAvroConverter.create()
          .codecFactory(CodecFactory.fromString(config.avroConfig.compressionType))
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsConfigs(hdfsConfigs)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .skipDeletion(true)
          .convert();
    };
  }

  @SneakyThrows
  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesValidatorDwcaMessage message) {
    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    Set<String> interpretedTypes = config.interpretTypes;

    try {
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      Archive archive = DwcaUtils.fromLocationSkipValidation(inputPath);

      if (message.getPipelineSteps().isEmpty()) {
        PipelinesWorkflow.Graph<StepType> workflow;
        workflow = PipelinesWorkflow.getValidatorWorkflow();

        StepType type = StepType.VALIDATOR_DWCA_TO_VERBATIM;

        Set<String> steps =
            workflow.getAllNodesFor(Collections.singleton(type)).stream()
                .map(StepType::name)
                .collect(Collectors.toSet());

        message.setPipelineSteps(steps);
      }

      // Calculates and checks the existence of DwC Archive
      Set<String> allInterpretationAsString = getAllValidatorInterpretationAsString();

      interpretedTypes = DwcaUtils.getExtensionAsTerms(archive);
      interpretedTypes.addAll(allInterpretationAsString);
      interpretedTypes.remove(null);
    } catch (IllegalStateException | UnsupportedArchiveException ex) {
      Set<String> steps =
          PipelinesWorkflow.getOccurrenceWorkflow().getAllNodes().stream()
              .map(StepType::name)
              .collect(Collectors.toSet());
      message.setPipelineSteps(steps);
      log.warn(ex.getMessage(), ex);
    }

    // Common variables
    OccurrenceValidationReport report = message.getValidationReport().getOccurrenceReport();

    Long dwcaRecordsNumber = null;
    if (message.getDatasetType() == DatasetType.SAMPLING_EVENT) {
      dwcaRecordsNumber =
          RecordCountReader.builder()
              .stepConfig(config.stepConfig)
              .datasetKey(message.getDatasetUuid().toString())
              .attempt(message.getAttempt().toString())
              .metaFileName(config.metaFileName)
              .metricName(PipelinesVariables.Metrics.ARCHIVE_TO_ER_COUNT)
              .build()
              .get();
    }

    long dwcaOccurrenceRecordsNumber =
        RecordCountReader.builder()
            .stepConfig(config.stepConfig)
            .datasetKey(message.getDatasetUuid().toString())
            .attempt(message.getAttempt().toString())
            .metaFileName(config.metaFileName)
            .metricName(PipelinesVariables.Metrics.ARCHIVE_TO_OCC_COUNT)
            .build()
            .get();

    PipelinesVerbatimMessage.ValidationResult validationResult =
        new PipelinesVerbatimMessage.ValidationResult(
            tripletsValid(report),
            occurrenceIdsValid(report),
            null,
            dwcaOccurrenceRecordsNumber,
            dwcaRecordsNumber);

    // this is a deliberate hack (see issue https://github.com/gbif/pipelines/issues/885)
    DatasetType datasetType = message.getDatasetType();

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        interpretedTypes,
        message.getPipelineSteps(),
        null,
        message.getEndpointType(),
        null,
        validationResult,
        null,
        null,
        datasetType);
  }

  /**
   * For XML datasets triplets are always valid. For DwC-A datasets triplets are valid if there are
   * more than 0 unique triplets in the dataset, and exactly 0 triplets referenced by more than one
   * record.
   */
  private boolean tripletsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getUniqueTriplets() > 0
        && report.getCheckedRecords() - report.getRecordsWithInvalidTriplets()
        == report.getUniqueTriplets();
  }

  /**
   * For XML datasets occurrenceIds are always accepted. For DwC-A datasets occurrenceIds are valid
   * if each record has a unique occurrenceId.
   */
  private boolean occurrenceIdsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getCheckedRecords() > 0
        && report.getUniqueOccurrenceIds() == report.getCheckedRecords();
  }
}
