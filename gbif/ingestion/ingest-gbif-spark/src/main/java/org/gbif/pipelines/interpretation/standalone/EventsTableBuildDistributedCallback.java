package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.spark.Directories.EVENT_HDFS;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class EventsTableBuildDistributedCallback extends EventsTableBuildCallback {

  public EventsTableBuildDistributedCallback(
      PipelinesConfig pipelinesConfig,
      MessagePublisher publisher,
      String tableName,
      String sourceDirectory) {
    super(pipelinesConfig, publisher, null, tableName, sourceDirectory);
  }

  @Override
  protected void runPipeline(PipelinesEventsInterpretedMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "tablebuild",
        fileSystem,
        pipelinesConfig.getAirflowConfig().eventsTableBuildDag,
        StepType.EVENTS_HDFS_VIEW,
        List.of("--tableName=event", "--sourceDirectory=" + EVENT_HDFS));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
