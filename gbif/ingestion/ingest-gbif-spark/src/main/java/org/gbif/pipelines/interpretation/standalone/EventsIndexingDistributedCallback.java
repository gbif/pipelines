package org.gbif.pipelines.interpretation.standalone;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Indexing;

@Slf4j
public class EventsIndexingDistributedCallback extends EventsIndexingCallback {

  public EventsIndexingDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher, null);
  }

  @Override
  protected void runPipeline(PipelinesEventsInterpretedMessage message) throws Exception {

    Long recordsNumber = DistributedUtil.getRecordsNumber(pipelinesConfig, message, fileSystem);

    IndexSettings indexSettings =
        IndexSettings.create(
            DatasetType.SAMPLING_EVENT,
            pipelinesConfig.getIndexConfig(),
            httpClient,
            message.getDatasetUuid().toString(),
            message.getAttempt(),
            recordsNumber);

    log.info("Start the process. Message - {}", message);
    List<String> extraArgs =
        List.of(
            Indexing.ES_INDEX_NAME_ARG + "=" + indexSettings.getIndexName(),
            Indexing.ES_INDEX_NUMBER_OF_SHARDS_ARG + "=" + indexSettings.getNumberOfShards(),
            Indexing.ES_INDEX_ALIAS_ARG + "=" + indexSettings.getIndexAlias(),
            Indexing.ES_INDEX_DATASET_TYPE + "=" + DatasetType.SAMPLING_EVENT);

    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "event-indexing",
        fileSystem,
        pipelinesConfig.getAirflowConfig().eventsIndexingDag,
        StepType.EVENTS_INTERPRETED_TO_INDEX,
        extraArgs);
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
