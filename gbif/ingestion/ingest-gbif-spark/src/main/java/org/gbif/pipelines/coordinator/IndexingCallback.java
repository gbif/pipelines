package org.gbif.pipelines.coordinator;

import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.EsIndexUtils;
import org.gbif.pipelines.IndexSettings;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.spark.Directories;
import org.gbif.pipelines.spark.Indexing;

@Slf4j
public class IndexingCallback
    extends PipelinesCallback<PipelinesInterpretedMessage, PipelinesIndexedMessage>
    implements MessageCallback<PipelinesInterpretedMessage> {

  private String defaultIndexName = null;

  public IndexingCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.INTERPRETED_TO_INDEX;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    Indexing.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {

    initialiseIndex(message);

    Indexing.runIndexing(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getOccurrenceIndexAlias(),
        defaultIndexName,
        pipelinesConfig.getStandalone().getOccurrenceIndexSchema(),
        pipelinesConfig.getStandalone().getOccurrenceIndexNumberOfShards(),
        OccurrenceJsonRecord.class,
        Directories.OCCURRENCE_JSON);
  }

  private void initialiseIndex(PipelinesInterpretedMessage message) throws IOException {

    if (defaultIndexName != null) {
      return;
    }

    log.info("Initialising the index..");

    // check if the index exists, if not create it with the default name and alias
    String defaultIndexPrefix =
        pipelinesConfig.getIndexConfig().defaultPrefixName
            + "_"
            + pipelinesConfig.getIndexConfig().occurrenceVersion;

    // does the default index exist already ?
    Optional<String> indexName =
        IndexSettings.getIndexName(
            pipelinesConfig.getIndexConfig(), httpClient, defaultIndexPrefix);

    if (indexName.isEmpty()) {

      log.info("create the default index for small datasets..");
      String indexToBeCreated = defaultIndexPrefix + "_" + System.currentTimeMillis();
      // create the default index, and add the alias to it, if the index doesn't exist
      EsIndexUtils.createIndexAndAliasForDefault(
          Indexing.ElasticOptions.fromArgsAndConfig(
              pipelinesConfig,
              pipelinesConfig.getIndexConfig().occurrenceAlias,
              indexToBeCreated,
              pipelinesConfig.getIndexConfig().getOccurrenceSchemaPath(),
              message.getDatasetUuid().toString(),
              message.getAttempt(),
              pipelinesConfig.getStandalone().getOccurrenceIndexNumberOfShards()));
      defaultIndexName = indexToBeCreated;
    } else {
      defaultIndexName = indexName.get();
      log.info("index with the default name already exists {}", defaultIndexName);
    }

    assert defaultIndexName != null;
  }

  @Override
  protected String getMetaFileName() {
    return Indexing.METRICS_FILENAME;
  }

  @Override
  public Class<PipelinesInterpretedMessage> getMessageClass() {
    return PipelinesInterpretedMessage.class;
  }

  @Override
  public PipelinesIndexedMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        isStandalone() ? "STANDALONE" : "DISTRIBUTED",
        message.getExecutionId(),
        message.getEndpointType());
  }
}
