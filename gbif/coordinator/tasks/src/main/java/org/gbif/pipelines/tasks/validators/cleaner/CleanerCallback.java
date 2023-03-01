package org.gbif.pipelines.tasks.validators.cleaner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.PipelinesCleanerMessage;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/** Call back which is called when the {@link PipelinesCleanerMessage} is received. */
@Slf4j
@AllArgsConstructor
public class CleanerCallback extends AbstractMessageCallback<PipelinesCleanerMessage> {

  private final CleanerConfiguration config;
  private final ValidationWsClient validationClient;

  @Override
  public void handleMessage(PipelinesCleanerMessage message) {

    try (MDCCloseable mdc = MDC.putCloseable("datasetKey", message.getDatasetUuid().toString());
        MDCCloseable mdc1 = MDC.putCloseable("attempt", message.getAttempt().toString())) {

      UUID datasetUuid = message.getDatasetUuid();

      log.info("Deleting index/records and files");

      deleteFsData(datasetUuid);
      deleteHdfsData(datasetUuid);
      deleteEsData(datasetUuid);
      markDataAsDeleted(datasetUuid);
    }
  }

  public String getRouting() {
    return new PipelinesCleanerMessage().setValidator(config.validatorOnly).getRoutingKey();
  }

  private void deleteFsData(UUID datasetUuid) {
    log.info("Delete file system files");
    String pathToDelete = String.join("/", config.fsRootPath, datasetUuid.toString());
    boolean isDeleted = false;

    try (Stream<Path> stream = Files.walk(Paths.get(pathToDelete))) {
      isDeleted = stream.sorted(Comparator.reverseOrder()).map(Path::toFile).allMatch(File::delete);

    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
    }
    if (isDeleted) {
      log.info("Dataset files was deleted successfully from FS!");
    } else {
      log.warn("Dataset files was NOT deleted from FS!");
    }
  }

  private void deleteHdfsData(UUID datasetUuid) {
    log.info("Delete HDFS files");
    String pathToDelete = String.join("/", config.hdfsRootPath, datasetUuid.toString());
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    boolean isDeleted = FsUtils.deleteIfExist(hdfsConfigs, pathToDelete);
    if (isDeleted) {
      log.info("Dataset files was deleted successfully from HDFS!");
    } else {
      log.warn("Dataset files was NOT deleted from HDFS!");
    }
  }

  private void deleteEsData(UUID datasetUuid) {
    log.info("Delete elasticsearch index/documents");

    EsConfig esConfig = EsConfig.from(config.esHosts);

    // Delete records by delete query and return list of indices names
    Set<String> indices =
        EsIndex.deleteRecordsByDatasetId(
            esConfig,
            config.esAliases,
            datasetUuid.toString(),
            idxName -> !idxName.startsWith("."),
            config.esSearchQueryTimeoutSec,
            config.esSearchQueryAttempts);

    // Delete index by name, must be only one index name in the list
    indices.stream()
        .filter(idxName -> idxName.startsWith(datasetUuid.toString()))
        .forEach(idxName -> EsIndex.deleteIndex(esConfig, idxName));

    if (indices.isEmpty()) {
      log.warn("Dataset index/records was NOT deleted from elasticseach!");
    } else {
      log.info("Dataset index/records was deleted successfully from elasticseach!");
    }
  }

  private void markDataAsDeleted(UUID validationKey) {
    log.info("Mark DB record as deleted for validation {}", validationKey);
    validationClient.delete(validationKey);
  }
}
