package org.gbif.pipelines.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.gbif.pipelines.common.configs.RegistryConfiguration;
import org.gbif.pipelines.tasks.MachineTag;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GbifApi {

  private static final Retry RETRY =
      Retry.of(
          "apiCall",
          RetryConfig.custom()
              .maxAttempts(7)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(6)))
              .build());

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Get number of record using Occurrence API */
  @SneakyThrows
  public static long getIndexSize(
      HttpClient httpClient, RegistryConfiguration config, String datasetId) {
    int nano = LocalDateTime.now().getNano();
    String url = config.wsUrl + "/occurrence/search?limit=0&datasetKey=" + datasetId + "&_" + nano;
    HttpResponse response = executeGet(httpClient, url);
    return MAPPER.readTree(response.getEntity().getContent()).findValue("count").asLong();
  }

  @SneakyThrows
  public static Optional<String> getMachineTagValue(
      HttpClient httpClient, RegistryConfiguration config, String datasetId, String tagName) {
    String url = config.wsUrl + "/dataset/" + datasetId + "/machineTag";
    HttpResponse response = executeGet(httpClient, url);

    List<MachineTag> machineTags =
        MAPPER.readValue(response.getEntity().getContent(), new TypeReference<>() {});

    return machineTags.stream()
        .filter(x -> x.getName().equals(tagName))
        .map(MachineTag::getValue)
        .findFirst();
  }

  @SneakyThrows
  public static String getInstallationKey(
      HttpClient httpClient, RegistryConfiguration config, String datasetId) {
    String url = config.wsUrl + "/dataset/" + datasetId;
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

    Supplier<HttpResponse> s =
        () -> {
          try {
            return httpClient.execute(new HttpGet(url));
          } catch (IOException ex) {
            throw new PipelinesException(ex);
          }
        };

    HttpResponse response = Retry.decorateSupplier(RETRY, s).get();
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new PipelinesException(
          "GBIF API exception " + response.getStatusLine().getReasonPhrase());
    }
    return response;
  }
}
