package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.spark.Constants.DATASET_TYPE_ARG;
import static org.gbif.pipelines.spark.Constants.SOURCE_DIRECTORY_ARG;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.Directories;

@Slf4j
public class OccurrenceTableBuildDistributedCallback extends OccurrenceTableBuildCallback {

  public OccurrenceTableBuildDistributedCallback(
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
        "occurrence-tablebuild",
        fileSystem,
        pipelinesConfig.getAirflowConfig().tableBuildDag,
        StepType.HDFS_VIEW,
        List.of(
            DATASET_TYPE_ARG + "=" + DatasetType.OCCURRENCE,
            SOURCE_DIRECTORY_ARG + "=" + Directories.OCCURRENCE_HDFS));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
