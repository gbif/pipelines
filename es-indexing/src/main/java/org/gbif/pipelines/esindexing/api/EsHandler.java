package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.client.EsConfig;
import org.gbif.pipelines.esindexing.common.SettingsType;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.elasticsearch.client.Response;

import static org.gbif.pipelines.esindexing.api.EsService.getIndexesByAlias;
import static org.gbif.pipelines.esindexing.api.EsService.swapIndexes;
import static org.gbif.pipelines.esindexing.api.EsService.updateIndexSettings;

public class EsHandler {

  private static final String INDEX_SEPARATOR = "_";

  private EsHandler() {}

  public static String createIndex(EsConfig config, String datasetId, int attempt) {
    Objects.requireNonNull(config, "ES configuration is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetId), "dataset id is required");

    final String idxName = datasetId + INDEX_SEPARATOR + attempt;

    Response response = null;
    try (EsClient esClient = EsClient.from(config)) {
      return EsService.createIndexWithSettings(esClient, idxName, SettingsType.INDEXING);
    }
  }

  public static void swapIndexInAlias(EsConfig config, String alias, String index) {
    Objects.requireNonNull(config, "ES configuration is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(alias), "alias is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");

    // get dataset id
    String datasetId = getDatasetIdFromIndex(index);

    try (EsClient esClient = EsClient.from(config)) {
      // change index settings to search settings
      updateIndexSettings(esClient, index, SettingsType.SEARCH);

      // check if there are indexes to remove
      Set<String> idxToRemove = getIndexesByAlias(esClient, getDatasetIndexesPattern(datasetId), alias);

      // swap the indexes
      swapIndexes(esClient, index, alias, idxToRemove);
    }
  }

  private static String getDatasetIndexesPattern(String datasetId) {
    return datasetId + INDEX_SEPARATOR + "*";
  }

  private static String getDatasetIdFromIndex(String index) {
    String[] pieces = index.split(INDEX_SEPARATOR);

    if (pieces.length != 2) {
      throw new IllegalArgumentException("index has to follow the pattern \"dataset_attempt\"");
    }

    return pieces[0];
  }

}
