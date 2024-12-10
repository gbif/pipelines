package org.gbif.pipelines.tasks.verbatims.fragmenter;

import static org.gbif.pipelines.common.utils.HdfsUtils.buildOutputPath;
import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.pipelines.common.utils.PathUtil.buildXmlInputPath;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesFragmenterMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.airflow.AppName;
import org.gbif.pipelines.common.process.AirflowSparkLauncher;
import org.gbif.pipelines.common.process.BeamParametersBuilder;
import org.gbif.pipelines.common.process.BeamParametersBuilder.BeamParameters;
import org.gbif.pipelines.common.process.RecordCountReader;
import org.gbif.pipelines.common.process.SparkDynamicSettings;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.DatasetTypePredicate;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.fragmenter.FragmentPersister;
import org.gbif.pipelines.fragmenter.strategy.DwcaStrategy;
import org.gbif.pipelines.fragmenter.strategy.Strategy;
import org.gbif.pipelines.fragmenter.strategy.XmlStrategy;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesInterpretedMessage} is received. */
@Slf4j
@Builder
public class FragmenterCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements StepHandler<PipelinesInterpretedMessage, PipelinesFragmenterMessage> {

  private static final StepType TYPE = StepType.FRAGMENTER;

  private final FragmenterConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
  private final ExecutorService executor;
  private final Connection hbaseConnection;
  private final KeygenConfig keygenConfig;

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesFragmenterMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .config(config)
        .stepType(TYPE)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    return new PipelinesInterpretedMessage().getRoutingKey() + ".*";
  }

  @Override
  public Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      try {

        long recordsNumber =
            RecordCountReader.builder()
                .stepConfig(config.stepConfig)
                .datasetKey(message.getDatasetUuid().toString())
                .attempt(message.getAttempt().toString())
                .messageNumber(message.getNumberOfRecords())
                .metaFileName(new InterpreterConfiguration().metaFileName)
                .metricName(Metrics.BASIC_RECORDS_COUNT + Metrics.ATTEMPTED)
                .alternativeMetricName(Metrics.UNIQUE_GBIF_IDS_COUNT + Metrics.ATTEMPTED)
                .build()
                .get();

        log.info("Start the process. Message - {}", message);
        if (StepRunner.DISTRIBUTED.name().equalsIgnoreCase(message.getRunner())
            && DatasetTypePredicate.isEndpointDwca(message.getEndpointType())
            && config.switchRecordsNumber <= recordsNumber) {
          runDistributed(message, recordsNumber);
        } else {
          runLocal(message);
        }
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        throw new IllegalStateException(
            "Failed interpretation on " + message.getDatasetUuid().toString(), ex);
      }
    };
  }

  private void runDistributed(PipelinesInterpretedMessage message, long recordsNumber) {
    BeamParameters beamParameters = BeamParametersBuilder.verbatimFragmenter(config, message);

    // Spark dynamic settings
    boolean useMemoryExtraCoef =
        config.sparkConfig.extraCoefDatasetSet.contains(message.getDatasetUuid().toString());
    SparkDynamicSettings sparkDynamicSettings =
        SparkDynamicSettings.create(config.sparkConfig, recordsNumber, useMemoryExtraCoef);

    // App name
    String sparkAppName = AppName.get(TYPE, message.getDatasetUuid(), message.getAttempt());

    // Submit
    AirflowSparkLauncher.builder()
        .airflowConfiguration(config.airflowConfig)
        .sparkStaticConfiguration(config.sparkConfig)
        .sparkDynamicSettings(sparkDynamicSettings)
        .beamParameters(beamParameters)
        .sparkAppName(sparkAppName)
        .build()
        .submitAwaitVoid();
  }

  private void runLocal(PipelinesInterpretedMessage message) {
    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    Strategy strategy;
    Path pathToArchive;

    if (DatasetTypePredicate.isEndpointDwca(message.getEndpointType())) {
      strategy = DwcaStrategy.create();
      pathToArchive = buildDwcaInputPath(config.dwcaArchiveRepository, datasetId);
    } else {
      String subdir =
          getXmlSubdir(
              message.getEndpointType(),
              config.xmlArchiveRepositoryXml,
              config.xmlArchiveRepositoryAbcd);
      strategy = XmlStrategy.create();
      pathToArchive =
          buildXmlInputPath(config.xmlArchiveRepository, subdir, datasetId, attempt.toString());
    }

    boolean useSync = message.getNumberOfRecords() < config.asyncThreshold;

    log.info("Running fragmenter in asych mode: {} ...", useSync);

    long result =
        FragmentPersister.builder()
            .strategy(strategy)
            .endpointType(message.getEndpointType())
            .datasetKey(datasetId.toString())
            .attempt(attempt)
            .tableName(config.hbaseFragmentsTable)
            .hbaseConnection(hbaseConnection)
            .executor(executor)
            .useOccurrenceId(message.getValidationResult().isOccurrenceIdValid())
            .useTriplet(message.getValidationResult().isTripletValid())
            .pathToArchive(pathToArchive)
            .keygenConfig(keygenConfig)
            .useSyncMode(useSync)
            .backPressure(config.backPressure)
            .batchSize(config.batchSize)
            .generateIdIfAbsent(config.generateIdIfAbsent)
            .build()
            .persist();

    createMetafile(datasetId.toString(), attempt.toString(), result);

    log.info("Result - {} records", result);
  }

  @Override
  public PipelinesFragmenterMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesFragmenterMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps());
  }

  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    Set<String> steps = message.getPipelineSteps();

    boolean isCorrect = steps.contains(TYPE.name());
    if (!isCorrect) {
      log.info("Skipping, because expected step is {}", TYPE);
    }
    return isCorrect;
  }

  /** Create yaml file with total number of converted records */
  private void createMetafile(String datasetId, String attempt, long numberOfRecords) {
    try {
      org.apache.hadoop.fs.Path path =
          buildOutputPath(
              config.stepConfig.repositoryPath, datasetId, attempt, config.metaFileName);
      HdfsConfigs hdfsConfigs =
          HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
      FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(path.toString());
      String info = Metrics.FRAGMENTER_COUNT + ": " + numberOfRecords + "\n";
      FsUtils.createFile(fs, path, info);
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
    }
  }

  private String getXmlSubdir(EndpointType endpointType, String xmlSubdir, String abcdSubdir) {
    switch (endpointType) {
      case BIOCASE_XML_ARCHIVE:
        return abcdSubdir;
      case BIOCASE:
      case DIGIR:
      case TAPIR:
      case DIGIR_MANIS:
        return xmlSubdir;
      default:
        throw new IllegalArgumentException("EndpointType doesn't have mapping to subdirctory!");
    }
  }
}
