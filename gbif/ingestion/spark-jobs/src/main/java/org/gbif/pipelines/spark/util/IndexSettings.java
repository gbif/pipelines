package org.gbif.pipelines.spark.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.pipelines.core.config.model.IndexConfig;

@Slf4j
@Getter
public class IndexSettings {

  private final Integer numberOfShards;
  private final String indexName;
  private final String indexAlias;

  private IndexSettings(
      DatasetType datasetType,
      IndexConfig indexConfig,
      HttpClient httpClient,
      String datasetId,
      Integer attempt,
      long recordsNumber)
      throws IOException {
    this.indexName =
        computeIndexName(
            datasetType,
            indexConfig,
            httpClient,
            datasetId,
            attempt,
            recordsNumber,
            Instant.now().toEpochMilli());
    this.numberOfShards = computeNumberOfShards(indexConfig, indexName, recordsNumber);
    switch (datasetType) {
      case OCCURRENCE -> this.indexAlias = indexConfig.getOccurrenceAlias();
      case SAMPLING_EVENT -> this.indexAlias = indexConfig.getEventAlias();
      default -> throw new IllegalStateException("Unexpected value: " + datasetType);
    }
  }

  private IndexSettings(String indexName, Integer numberOfShards) {
    this.numberOfShards = numberOfShards;
    this.indexName = indexName;
    this.indexAlias = null;
  }

  public static IndexSettings create(
      DatasetType datasetType,
      IndexConfig indexConfig,
      HttpClient httpClient,
      String datasetId,
      Integer attempt,
      long recordsNumber)
      throws IOException {

    return new IndexSettings(
        datasetType, indexConfig, httpClient, datasetId, attempt, recordsNumber);
  }

  public static IndexSettings create(String indexName, Integer numberOfShards) {
    return new IndexSettings(indexName, numberOfShards);
  }

  /**
   * Computes ES index name.
   *
   * <p>Strategy: 1. Independent index if dataset is large (records >= threshold) 2. Otherwise reuse
   * default index if available 3. Otherwise create new default index with timestamp
   */
  public static String computeIndexName(
      DatasetType datasetType,
      IndexConfig indexConfig,
      HttpClient httpClient,
      String datasetId,
      int attempt,
      long recordsNumber,
      long timestamp)
      throws IOException {

    String indexVersion = resolveIndexVersion(indexConfig, datasetType);
    String indexAlias = resolveIndexAlias(indexConfig, datasetType);

    if (recordsNumber >= indexConfig.getBigIndexIfRecordsMoreThan()) {
      return buildIndependentIndexName(datasetId, attempt, indexVersion, timestamp);
    }

    String defaultPrefix = indexConfig.defaultPrefixName + "_" + indexVersion;
    String indexName =
        getExistingDefaultIndexName(indexConfig, httpClient, indexAlias, defaultPrefix)
            .orElse(defaultPrefix + "_" + timestamp);

    log.info("ES Index name - {}", indexName);
    return indexName;
  }

  /** Computes ES index name for datasets that must always use an independent (dedicated) index. */
  public static String computeLargeIndexName(
      DatasetType datasetType,
      IndexConfig indexConfig,
      String datasetId,
      int attempt,
      long timestamp) {

    String indexVersion = resolveIndexVersion(indexConfig, datasetType);
    return buildIndependentIndexName(datasetId, attempt, indexVersion, timestamp);
  }

  private static String resolveIndexVersion(IndexConfig config, DatasetType datasetType) {
    return switch (datasetType) {
      case OCCURRENCE -> config.occurrenceVersion;
      case SAMPLING_EVENT -> config.eventVersion;
      default -> throw new IllegalStateException("Unexpected value: " + datasetType);
    };
  }

  public static String resolveIndexAlias(IndexConfig config, DatasetType datasetType) {
    return switch (datasetType) {
      case OCCURRENCE -> config.occurrenceAlias;
      case SAMPLING_EVENT -> config.eventAlias;
      default -> throw new IllegalStateException("Unexpected value: " + datasetType);
    };
  }

  private static String buildIndependentIndexName(
      String datasetId, int attempt, String version, long timestamp) {
    return datasetId + "_" + attempt + "_" + version + "_" + timestamp;
  }

  /**
   * Computes number of index shards:
   *
   * <pre>
   * 1) in case of default index -> config.indexDefSize / config.indexRecordsPerShard
   * 2) in case of independent index -> recordsNumber / config.indexRecordsPerShard
   * </pre>
   */
  public static int computeNumberOfShards(
      IndexConfig indexConfig, String indexName, long recordsNumber) {
    if (indexName.startsWith(indexConfig.defaultPrefixName)) {
      int s =
          (int) Math.ceil((double) indexConfig.defaultSize / (double) indexConfig.recordsPerShard);

      // Add extra shard to accumulate deleted documents
      return indexConfig.defaultExtraShard ? s + 1 : s;
    }

    double shards = recordsNumber / (double) indexConfig.recordsPerShard;
    shards = Math.max(shards, 1d);
    boolean isCeil = (shards - Math.floor(shards)) > 0.25d;
    return isCeil ? (int) Math.ceil(shards) : (int) Math.floor(shards);
  }

  /**
   * Returns the name of the default index to use that is available to write to and is below the
   * size threshold. Will return Optional.empty() if a new index is required.
   *
   * @param indexConfig
   * @param httpClient
   * @param alias
   * @return
   * @throws IOException
   */
  public static Optional<String> getExistingDefaultIndexName(
      IndexConfig indexConfig, HttpClient httpClient, String alias, String defaultNamePrefix)
      throws IOException {

    ObjectMapper MAPPER = new ObjectMapper();
    // ignore unrecognised
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // "http://localhost:9200/_cat/aliases/%s?format=json",
    String getByAlias =
        String.format(indexConfig.defaultIndexCatUrl + "/_cat/aliases/%s?format=json", alias);
    HttpUriRequest httpGetByAlias = new HttpGet(getByAlias);
    HttpResponse responseByAlias = httpClient.execute(httpGetByAlias);
    if (responseByAlias.getStatusLine().getStatusCode() != 200) {
      throw new IOException(
          "ES _cat/aliases API exception " + responseByAlias.getStatusLine().getReasonPhrase());
    }

    // gets a list of all indexes in this alias
    List<String> defaultIndexes =
        MAPPER
            .readValue(
                responseByAlias.getEntity().getContent(), new TypeReference<List<EsCatIndex>>() {})
            .stream()
            .map(EsCatIndex::getName)
            .filter(name -> name.startsWith(defaultNamePrefix))
            .toList();

    // get a list of indexes matching
    String getByNamePrefix =
        String.format(
            indexConfig.defaultIndexCatUrl
                + "/_cat/indices/%s*?v&h=docs.count,index&s=docs.count:asc&format=json",
            defaultNamePrefix);
    HttpUriRequest httpGetByPrefix = new HttpGet(getByNamePrefix);
    HttpResponse responseByPrefix = httpClient.execute(httpGetByPrefix);
    if (responseByPrefix.getStatusLine().getStatusCode() != 200) {
      throw new IOException(
          "ES _cat/indices API exception " + responseByPrefix.getStatusLine().getReasonPhrase());
    }

    // look for indices matching the default prefix and with size less than the threshold
    List<EsCatIndex> defaultIndices =
        MAPPER
            .readValue(
                responseByPrefix.getEntity().getContent(), new TypeReference<List<EsCatIndex>>() {})
            .stream()
            .filter(
                index ->
                    defaultIndexes.contains(index.getName())
                        && index.getCount() <= indexConfig.defaultNewIfSize)
            .toList();

    if (!defaultIndices.isEmpty()) {
      return Optional.of(defaultIndices.get(0).getName());
    }
    return Optional.empty();
  }
}

@Getter
@Setter
class EsCatIndex implements Serializable {

  @Serial private static final long serialVersionUID = 7134020816642786944L;

  @JsonProperty("docs.count")
  private long count;

  @JsonProperty("index")
  private String name;
}
