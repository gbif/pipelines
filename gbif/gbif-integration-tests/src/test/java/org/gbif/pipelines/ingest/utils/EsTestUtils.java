package org.gbif.pipelines.ingest.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.elasticsearch.client.Response;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipeline;
import org.gbif.pipelines.ingest.resources.EsServer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsTestUtils {
  public static final String MATCH_QUERY = "{\"query\":{\"match\":{\"%s\":\"%s\"}}}";
  // default number of records per dataset
  public static final int DEFAULT_REC_DATASET = 10;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(Map.class);

  public static EsIndexingPipelineOptions createPipelineOptions(
      EsServer server, String datasetKey, String idxName, String alias, int attempt) {
    String propertiesPath =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("data7/ingest/pipelines.yaml")
            .getPath();
    String[] args = {
      "--esIndexName=" + idxName,
      "--datasetId=" + datasetKey,
      "--attempt=" + attempt,
      "--esAlias=" + alias,
      "--indexRefreshInterval=1ms",
      "--esHosts=" + server.getServerAddress(),
      "--esSchemaPath=dataset-mapping.json",
      "--properties=" + propertiesPath
    };
    return PipelineOptionsFactory.fromArgs(args).as(EsIndexingPipelineOptions.class);
  }

  public static Runnable indexingPipeline(
      EsServer server, EsIndexingPipelineOptions options, long numRecordsToIndex, String msg) {
    return () -> {
      String document =
          "{\"datasetKey\" : \""
              + options.getDatasetId()
              + "\", \"crawlId\" : "
              + options.getAttempt()
              + ", \"msg\": \"%s\"}";

      LongStream.range(0, numRecordsToIndex)
          .forEach(
              i ->
                  EsService.indexDocument(
                      server.getEsClient(),
                      options.getEsIndexName(),
                      i + options.getDatasetId().hashCode(),
                      String.format(document, msg + " " + i)));
      EsService.refreshIndex(server.getEsClient(), options.getEsIndexName());
    };
  }

  public static long countDocumentsFromQuery(EsServer server, String idxName, String query) {
    Response response = EsService.executeQuery(server.getEsClient(), idxName, query);
    try {
      return READER
          .readTree(response.getEntity().getContent())
          .get("hits")
          .get("total")
          .get("value")
          .asLong();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void indexDatasets(
      EsServer server,
      List<String> datasets,
      int attempt,
      String indexName,
      String alias,
      long recordsPerDataset) {
    datasets.forEach(
        dataset -> {
          EsIndexingPipelineOptions options =
              createPipelineOptions(
                  server,
                  dataset,
                  Strings.isNullOrEmpty(indexName) ? dataset + "_" + attempt : indexName,
                  alias,
                  attempt);
          InterpretedToEsIndexExtendedPipeline.run(
              options,
              indexingPipeline(server, options, recordsPerDataset, options.getEsIndexName()));
        });
  }

  public static void indexDatasets(
      EsServer server, List<String> datasets, int attempt, String indexName, String alias) {
    indexDatasets(server, datasets, attempt, indexName, alias, DEFAULT_REC_DATASET);
  }
}
