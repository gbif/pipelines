package org.gbif.pipelines.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
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
        MAPPER.readValue(
            response.getEntity().getContent(), new TypeReference<List<MachineTag>>() {});

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
    HttpResponse response = httpClient.execute(new HttpGet(url));
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new PipelinesException(
          "GBIF API exception " + response.getStatusLine().getReasonPhrase());
    }
    return response;
  }
}
