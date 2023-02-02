package au.org.ala.pipelines.util;

import static org.gbif.pipelines.estools.service.EsService.swapIndexes;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsConstants.Field;
import org.gbif.pipelines.estools.service.EsConstants.Indexing;
import org.gbif.pipelines.estools.service.EsService;

/** Utilities for creating elastic search indices and aliases. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ElasticsearchTools {

  /** Connects to Elasticsearch instance and creates an index, if index doesn't exist */
  public static void createIndexAndAliasForDefault(EsIndexingPipelineOptions options) {
    EsConfig config =
        EsConfig.from(options.getEsUsername(), options.getEsPassword(), options.getEsHosts());
    IndexParams params = createIndexParams(options);

    log.info("Creating index from params: {}", params);
    try (EsClient esClient = EsClient.from(config)) {
      if (!EsService.existsIndex(esClient, params.getIndexName())) {
        EsService.createIndex(esClient, params);
        addIndexAliasForDefault(esClient, options);
      }
    }
  }

  /** Add alias to index if the index is default/regular (it will contain many datasets) */
  private static void addIndexAliasForDefault(
      EsClient esClient, EsIndexingPipelineOptions options) {
    String index = options.getEsIndexName();
    Objects.requireNonNull(index, "index are required");
    if (!index.startsWith(options.getDatasetId())) {
      Set<String> aliases = new HashSet<>(Arrays.asList(options.getEsAlias()));

      Objects.requireNonNull(aliases, "aliases are required");

      Set<String> validAliases =
          aliases.stream()
              .filter(alias -> !Strings.isNullOrEmpty(alias))
              .collect(Collectors.toSet());
      Preconditions.checkArgument(!validAliases.isEmpty(), "aliases are required");

      swapIndexes(esClient, validAliases, Collections.singleton(index), Collections.emptySet());
    }
  }

  private static IndexParams createIndexParams(EsIndexingPipelineOptions options) {
    Path mappingsPath = Paths.get(options.getEsSchemaPath());

    boolean independentIndex = options.getEsIndexName().startsWith(options.getDatasetId());

    Map<String, String> settings = new HashMap<>(5);
    settings.put(
        Field.INDEX_REFRESH_INTERVAL,
        independentIndex ? Indexing.REFRESH_INTERVAL : options.getIndexRefreshInterval());
    settings.put(Field.INDEX_NUMBER_SHARDS, options.getIndexNumberShards().toString());
    settings.put(
        Field.INDEX_NUMBER_REPLICAS,
        independentIndex ? Indexing.NUMBER_REPLICAS : options.getIndexNumberReplicas().toString());
    settings.put(Field.INDEX_ANALYSIS, Indexing.ANALYSIS);
    settings.put(Field.INDEX_MAX_RESULT_WINDOW, options.getIndexMaxResultWindow().toString());

    return IndexParams.builder()
        .indexName(options.getEsIndexName())
        .datasetKey(options.getDatasetId())
        .attempt(options.getAttempt())
        .pathMappings(mappingsPath)
        .settings(settings)
        .build();
  }
}
