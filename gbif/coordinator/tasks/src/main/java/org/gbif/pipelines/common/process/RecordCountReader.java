package org.gbif.pipelines.common.process;

import java.io.IOException;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;

@Slf4j
@Builder(builderClassName = "create")
public class RecordCountReader {

  private final StepConfiguration stepConfig;
  private final String datasetKey;
  private final String attempt;
  private final String metaFileName;
  private final String metricName;
  private final String alternativeMetricName;
  private final Long messageNumber;
  @Builder.Default private final boolean skipIf = false;

  public long get() throws IOException {

    String metaPath =
        String.join("/", stepConfig.repositoryPath, datasetKey, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);

    if (skipIf && !HdfsUtils.exists(hdfsConfigs, metaPath)) {
      return 0L;
    }

    Optional<Long> fileNumber = HdfsUtils.getLongByKey(hdfsConfigs, metaPath, metricName);
    if (alternativeMetricName != null && fileNumber.isEmpty()) {
      fileNumber = HdfsUtils.getLongByKey(hdfsConfigs, metaPath, alternativeMetricName);
    }

    if (messageNumber == null && fileNumber.isEmpty()) {
      throw new IllegalArgumentException(
          "Please check metadata yaml file or message records number, recordsNumber can't be null or empty! File: "
              + metaFileName);
    }

    if (messageNumber == null) {
      return fileNumber.get();
    }

    if (fileNumber.isEmpty() || messageNumber > fileNumber.get()) {
      return messageNumber;
    }

    return fileNumber.get();
  }
}
