package org.gbif.pipelines.interpretation.standalone;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class TableBuildDistributedCallback extends TableBuildCallback {

  public TableBuildDistributedCallback(
      PipelinesConfig pipelinesConfig,
      MessagePublisher publisher,
      String tableName,
      String sourceDirectory) {
    super(pipelinesConfig, publisher, null, tableName, sourceDirectory);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "tablebuild",
        fileSystem,
        pipelinesConfig.getAirflowConfig().tableBuildDag,
        StepType.HDFS_VIEW,
        List.of("--tableName=occurrence", "--sourceDirectory=hdfs"));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
