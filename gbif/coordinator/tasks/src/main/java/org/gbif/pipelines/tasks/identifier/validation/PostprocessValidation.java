package org.gbif.pipelines.tasks.identifier.validation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.ToDoubleFunction;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MachineTag;
import org.gbif.pipelines.tasks.identifier.IdentifierConfiguration;

@Slf4j
@Builder
public class PostprocessValidation {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final PipelinesVerbatimMessage message;
  private final IdentifierConfiguration config;
  private final CloseableHttpClient httpClient;

  public void validate() throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = config.metaFileName;
    String metaPath =
        String.join("/", config.stepConfig.repositoryPath, datasetId, attempt, metaFileName);
    log.info("Getting records number from the file - {}", metaPath);

    Double threshold = getMachineTagValue().orElse(config.idThresholdPercent);

    ToDoubleFunction<String> getMetricFn =
        m -> {
          try {
            HdfsConfigs hdfsConfigs =
                HdfsConfigs.create(
                    config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
            return HdfsUtils.getDoubleByKey(hdfsConfigs, metaPath, m + Metrics.ATTEMPTED)
                .orElse(0d);
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    double totalCount = getMetricFn.applyAsDouble(Metrics.GBIF_ID_RECORDS_COUNT);
    double absentIdCount = getMetricFn.applyAsDouble(Metrics.ABSENT_GBIF_ID_COUNT);

    if (totalCount == 0d) {
      log.error("Interpreted totalCount {}, invalid absentIdCount {}", totalCount, absentIdCount);
      throw new IllegalArgumentIOException("No records with valid GBIF ID!");
    }

    if (absentIdCount != 0d) {
      double absentPercent = absentIdCount * 100 / totalCount;

      if (absentPercent > threshold) {
        log.error(
            "GBIF IDs hit maximum allowed threshold: allowed - {}%, duplicates - {}%",
            threshold, absentPercent);
        throw new IllegalArgumentIOException("GBIF IDs hit maximum allowed threshold");
      } else {
        log.warn(
            "GBIF IDs current duplicates rate: allowed - {}%, duplicates - {}%",
            threshold, absentPercent);
      }
    }
  }

  @SneakyThrows
  private Optional<Double> getMachineTagValue() {
    String url =
        config.stepConfig.registry.wsUrl + "/dataset/" + message.getDatasetUuid() + "/machineTag";
    HttpResponse response = httpClient.execute(new HttpGet(url));
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException("GBIF API exception " + response.getStatusLine().getReasonPhrase());
    }

    List<MachineTag> machineTags =
        MAPPER.readValue(
            response.getEntity().getContent(), new TypeReference<List<MachineTag>>() {});

    return machineTags.stream()
        .filter(x -> x.getName().equals("idThresholdPercent"))
        .map(MachineTag::getValue)
        .map(Double::parseDouble)
        .findFirst();
  }
}
