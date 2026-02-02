package org.gbif.pipelines.coordinator;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.StandaloneConfig;
import org.gbif.pipelines.spark.ValidateIdentifiers;

@Slf4j
@Builder
public class PostprocessValidation {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final StandaloneConfig config;
  private final PipelinesVerbatimMessage message;
  private final CloseableHttpClient httpClient;
  private final FileSystem fileSystem;
  private final String outputPath;

  private static final Retry RETRY =
      Retry.of(
          "apiCall",
          RetryConfig.custom()
              .maxAttempts(20)
              .retryExceptions(
                  JsonParseException.class,
                  IOException.class,
                  TimeoutException.class,
                  PipelinesException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .build());

  public IdentifierValidationResult validate() throws IOException {
    if (useThresholdSkipTagValue() || ignoreChecklists() || skipInstallationKey()) {
      String validationMessage = "Skip validation because machine tag id_threshold_skip=true";
      return new IdentifierValidationResult(0d, 0d, true, validationMessage);
    } else {
      return validateThreshold();
    }
  }

  private IdentifierValidationResult validateThreshold() throws IOException {
    String datasetId = message.getDatasetUuid().toString();
    String attempt = Integer.toString(message.getAttempt());
    String metaFileName = ValidateIdentifiers.METRICS_FILENAME;
    String metaPath = String.join("/", outputPath, datasetId, attempt, metaFileName);
    log.debug("Getting records number from the file - {}", metaPath);

    Double threshold = getThresholdTagValue().orElse(config.getIdThresholdPercent());

    ToLongFunction<String> getMetricFn =
        metric -> {
          try {
            String value =
                getValueByKey(fileSystem, metaPath, metric + PipelinesVariables.Metrics.ATTEMPTED)
                    .orElse("0");
            return Long.parseLong(value);
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    long totalCount = getMetricFn.applyAsLong(PipelinesVariables.Metrics.GBIF_ID_RECORDS_COUNT);
    long absentIdCount = getMetricFn.applyAsLong(PipelinesVariables.Metrics.ABSENT_GBIF_ID_COUNT);
    long existingCount = getMetricFn.applyAsLong(PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT);

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
    return new IdentifierValidationResult(totalCount, absentIdCount, isValid, validationMessage);
  }

  /**
   * Reads a yaml file and returns value by key
   *
   * @param filePath to a yaml file
   * @param key to value in yaml
   */
  public static Optional<String> getValueByKey(FileSystem fs, String filePath, String key)
      throws IOException {
    Path fsPath = new Path(filePath);
    if (fs.exists(fsPath)) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fsPath), UTF_8))) {
        return br.lines()
            .map(x -> x.replace("\u0000", ""))
            .filter(y -> y.startsWith(key))
            .findFirst()
            .map(z -> z.replace(key + ": ", ""));
      }
    }
    return Optional.empty();
  }

  @SneakyThrows
  private Optional<Double> getThresholdTagValue() {
    String datasetKey = message.getDatasetUuid().toString();
    return getMachineTagValue(httpClient, datasetKey, "id_threshold_percent")
        .map(Double::parseDouble);
  }

  @SneakyThrows
  private boolean useThresholdSkipTagValue() {
    String datasetKey = message.getDatasetUuid().toString();
    return getMachineTagValue(httpClient, datasetKey, "id_threshold_skip")
        .map(Boolean::parseBoolean)
        .orElse(Boolean.FALSE);
  }

  @SneakyThrows
  private boolean skipInstallationKey() {
    String datasetKey = message.getDatasetUuid().toString();
    String installationKey = getInstallationKey(httpClient, datasetKey);
    boolean r = config.getSkipInstallationsList().contains(installationKey);
    if (r) {
      log.warn("Installation key {} is in the config skip list", datasetKey);
    }
    return r;
  }

  private boolean ignoreChecklists() {
    return message.getDatasetType() == DatasetType.CHECKLIST;
  }

  @SneakyThrows
  private long getApiRecords() {
    String datasetKey = message.getDatasetUuid().toString();
    return getIndexSize(httpClient, datasetKey);
  }

  /** Get number of record using Occurrence API */
  @SneakyThrows
  public long getIndexSize(HttpClient httpClient, String datasetId) {
    int nano = LocalDateTime.now().getNano();
    String url =
        config.getRegistry().getWsUrl()
            + "/occurrence/search?limit=0&datasetKey="
            + datasetId
            + "&_"
            + nano;
    HttpResponse response = executeGet(httpClient, url);
    return MAPPER.readTree(response.getEntity().getContent()).findValue("count").asLong();
  }

  @SneakyThrows
  public Optional<String> getMachineTagValue(
      HttpClient httpClient, String datasetId, String tagName) {
    String url = config.getRegistry().getWsUrl() + "/dataset/" + datasetId + "/machineTag";
    HttpResponse response = executeGet(httpClient, url);

    List<MachineTag> machineTags =
        MAPPER.readValue(response.getEntity().getContent(), new TypeReference<>() {});

    return machineTags.stream()
        .filter(x -> x.getName().equals(tagName))
        .map(MachineTag::getValue)
        .findFirst();
  }

  @SneakyThrows
  public String getInstallationKey(HttpClient httpClient, String datasetId) {
    String url = config.getRegistry().getWsUrl() + "/dataset/" + datasetId;
    HttpResponse response = executeGet(httpClient, url);

    Dataset dataset = MAPPER.readValue(response.getEntity().getContent(), Dataset.class);

    return dataset.installationKey;
  }

  @Getter
  @Setter
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class Dataset implements Serializable {

    private static final long serialVersionUID = 5148910816642786945L;

    @JsonProperty("installationKey")
    private String installationKey;
  }

  @SneakyThrows
  private static HttpResponse executeGet(HttpClient httpClient, String url) {

    Supplier<HttpResponse> supplier =
        () -> {
          try {
            return httpClient.execute(new HttpGet(url));
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    HttpResponse response = Retry.decorateSupplier(RETRY, supplier).get();
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new PipelinesException(
          "GBIF API exception " + response.getStatusLine().getReasonPhrase());
    }
    return response;
  }
}
