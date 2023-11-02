package org.gbif.pipelines.tasks.occurrences.identifier.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Optional;
import java.util.function.ToLongFunction;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.GbifApi;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.configs.RegistryConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.occurrences.identifier.IdentifierConfiguration;

@Slf4j
@Builder
public class PostprocessValidation {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final PipelinesVerbatimMessage message;
  private final IdentifierConfiguration config;
  private final CloseableHttpClient httpClient;

  public IdentifierValidationResult validate() throws IOException {
    if (useThresholdSkipTagValue() || ignoreChecklists() || skipInstallationKey()) {
      String validatonMessage = "Skip valiation because machine tag id_threshold_skip=true";
      return IdentifierValidationResult.create(0d, 0d, true, validatonMessage);
    } else {
      return validateThreshold();
    }
  }

  private IdentifierValidationResult validateThreshold() throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = config.metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);

    Double threshold = getThresholdTagValue().orElse(config.idThresholdPercent);

    ToLongFunction<String> getMetricFn =
        m -> {
          try {
            HdfsConfigs hdfsConfigs =
                HdfsConfigs.create(
                    config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
            return HdfsUtils.getLongByKey(hdfsConfigs, metaPath, m + Metrics.ATTEMPTED).orElse(0L);
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    long totalCount = getMetricFn.applyAsLong(Metrics.GBIF_ID_RECORDS_COUNT);
    long absentIdCount = getMetricFn.applyAsLong(Metrics.ABSENT_GBIF_ID_COUNT);
    long existingCount = getMetricFn.applyAsLong(Metrics.UNIQUE_GBIF_IDS_COUNT);

    if (totalCount == 0d) {
      log.error("Interpreted totalCount {}, invalid absentIdCount {}", totalCount, absentIdCount);
      throw new IllegalArgumentIOException("No records with valid GBIF ID!");
    }

    double absentPercent = (double) absentIdCount * 100 / totalCount;
    long apiRecords = getApiRecords();

    boolean isValid = true;
    String validationMessage = "No identifier issues";
    if (absentPercent > 0d && apiRecords > 0) {
      if (absentPercent > threshold && existingCount != apiRecords) {
        validationMessage =
            String.format(
                "GBIF ID problems exceed %.0f%% threshold: %.0f%% duplicates; %d total records; %d absent records",
                threshold, absentPercent, totalCount, absentIdCount);
        isValid = false;
      } else {
        validationMessage =
            String.format(
                "GBIF ID problems within %.0f%% threshold: %.0f%% duplicates; %d total records; %d absent records",
                threshold, absentPercent, totalCount, absentIdCount);
      }
    } else if (absentPercent == 100d) {
      validationMessage = "Skip ID validation: dataset has no API records and all IDs are new";
    } else if (absentPercent > 0d) {
      validationMessage =
          String.format("Dataset has no API records, but %.0f%% of IDs aren't new", absentPercent);
      isValid = false;
    }
    return IdentifierValidationResult.create(totalCount, absentIdCount, isValid, validationMessage);
  }

  @SneakyThrows
  private Optional<Double> getThresholdTagValue() {
    RegistryConfiguration registryConfiguration = config.stepConfig.registry;
    String datasetKey = message.getDatasetUuid().toString();
    return GbifApi.getMachineTagValue(
            httpClient, registryConfiguration, datasetKey, "id_threshold_percent")
        .map(Double::parseDouble);
  }

  @SneakyThrows
  private boolean useThresholdSkipTagValue() {
    if (config.idThresholdSkip) {
      return true;
    }
    RegistryConfiguration registryConfiguration = config.stepConfig.registry;
    String datasetKey = message.getDatasetUuid().toString();
    return GbifApi.getMachineTagValue(
            httpClient, registryConfiguration, datasetKey, "id_threshold_skip")
        .map(Boolean::parseBoolean)
        .orElse(Boolean.FALSE);
  }

  @SneakyThrows
  private boolean skipInstallationKey() {
    RegistryConfiguration registryConfiguration = config.stepConfig.registry;
    String datasetKey = message.getDatasetUuid().toString();
    String installationKey =
        GbifApi.getInstallationKey(httpClient, registryConfiguration, datasetKey);
    boolean r = config.skipInstallationsList.contains(installationKey);
    if (r) {
      log.info("Installation key {} is in the config skip list", datasetKey);
    }
    return r;
  }

  private boolean ignoreChecklists() {
    return config.ignoreChecklists && message.getDatasetType() == DatasetType.CHECKLIST;
  }

  @SneakyThrows
  private long getApiRecords() {
    RegistryConfiguration registryConfiguration = config.stepConfig.registry;
    String datasetKey = message.getDatasetUuid().toString();
    return GbifApi.getIndexSize(httpClient, registryConfiguration, datasetKey);
  }
}
