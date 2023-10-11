package org.gbif.pipelines.common.hdfs;

import java.io.IOException;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RecordCountReader {

  /**
   * Reads number of records from an archive-to-avro metadata file, verbatim-to-interpreted contains
   * attempted records count, which is not accurate enough
   */
  public static long get(StepConfiguration stepConfig, PipelinesInterpretationMessage message)
      throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = new InterpreterConfiguration().metaFileName;
    String metaPath = String.join("/", stepConfig.repositoryPath, datasetId, attempt, metaFileName);

    Long messageNumber = null;
    if (message instanceof PipelinesInterpretedMessage) {
      messageNumber = ((PipelinesInterpretedMessage) message).getNumberOfRecords();
    } else if (message instanceof PipelinesEventsInterpretedMessage) {
      messageNumber = ((PipelinesEventsInterpretedMessage) message).getNumberOfEventRecords();
    }

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);

    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(
            hdfsConfigs, metaPath, Metrics.BASIC_RECORDS_COUNT + Metrics.ATTEMPTED);
    if (!fileNumber.isPresent()) {
      fileNumber =
          HdfsUtils.getLongByKey(
              hdfsConfigs, metaPath, Metrics.UNIQUE_GBIF_IDS_COUNT + Metrics.ATTEMPTED);
    }

    if (messageNumber == null && !fileNumber.isPresent()) {
      throw new IllegalArgumentException(
          "Please check archive-to-avro metadata yaml file or message records number, recordsNumber can't be null or empty!");
    }

    if (messageNumber == null) {
      return fileNumber.get();
    }

    if (!fileNumber.isPresent() || messageNumber > fileNumber.get()) {
      return messageNumber;
    }

    return fileNumber.get();
  }
}
