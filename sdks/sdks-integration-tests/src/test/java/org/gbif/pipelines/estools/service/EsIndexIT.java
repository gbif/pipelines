package org.gbif.pipelines.estools.service;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.gbif.pipelines.estools.service.EsConstants.Field;
import static org.gbif.pipelines.estools.service.EsConstants.Searching;
import static org.gbif.pipelines.estools.service.EsConstants.Util.INDEX_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.elasticsearch.client.ResponseException;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link EsIndex}. */
public class EsIndexIT extends EsApiIntegration {

  private static final int DEFAULT_ATTEMPT = 1;
  private static final Function<String, String> IDX_NAME_FN =
      n -> n + INDEX_SEPARATOR + DEFAULT_ATTEMPT;
  private static final Function<String, String> SEARCH_FN = n -> n + INDEX_SEPARATOR + "*";
  private static final Function<String, String> ALIAS_FN = n -> n + INDEX_SEPARATOR + "_ALIAS";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createIndexTest() {

    String i = "create-index-test";
    String idxName = IDX_NAME_FN.apply(i);

    // create index
    String idxCreated =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().indexName(idxName).settingsType(INDEXING).build());

    // assert index created
    assertIndexWithSettingsAndIndexName(i, idxCreated);
  }

  @Test
  public void createIndexWithMappingsTest() {

    String i = "create-index-with-mappings-test";
    String idxName = IDX_NAME_FN.apply(i);

    // create index
    String idxCreated =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .indexName(idxName)
                .settingsType(INDEXING)
                .pathMappings(EsApiIntegration.TEST_MAPPINGS_PATH)
                .build());

    // assert index created
    assertIndexWithSettingsAndIndexName(i, idxCreated);

    // assert mappings
    JsonNode mappings =
        EsApiIntegration.getMappingsFromIndex(idxCreated).path(idxCreated).path(Field.MAPPINGS);
    assertTrue(mappings.path("properties").has("test"));
  }

  @Test
  public void swapIndexInEmptyAliasTest() {

    String i = "swap-index-in-empty-alias-test";
    String alias = ALIAS_FN.apply(i);
    String search = SEARCH_FN.apply(i);

    // create index
    String idxCreated =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().datasetKey(i).attempt(1).settingsType(INDEXING).build());

    // swap index
    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias), idxCreated);

    // assert result
    EsApiIntegration.assertSwapResults(idxCreated, search, alias, Collections.emptySet());

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idxCreated));
    EsApiIntegration.assertSearchSettings(idxCreated);
  }

  @Test
  public void swapIndexInAliasTest() {

    String i = "swap-index-in-empty-alias-test";
    String alias = ALIAS_FN.apply(i);
    String search = SEARCH_FN.apply(i);

    // create index
    String idx1 =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().datasetKey(i).attempt(1).settingsType(INDEXING).build());
    String idx2 =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().datasetKey(i).attempt(2).settingsType(INDEXING).build());
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2));

    // add the indexes to the alias
    EsApiIntegration.addIndexesToAlias(alias, initialIndexes);

    // create another index and swap it in the alias
    String idx3 =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().datasetKey(i).attempt(3).settingsType(INDEXING).build());
    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias), idx3);

    // alias should have only the last index created
    EsApiIntegration.assertSwapResults(idx3, search, alias, initialIndexes);

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx3));
    EsApiIntegration.assertSearchSettings(idx3);

    // create another index and swap it again
    String idx4 =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().datasetKey(i).attempt(4).settingsType(INDEXING).build());
    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias), idx4);

    // alias should have only the last index created
    EsApiIntegration.assertSwapResults(idx4, search, alias, Collections.singleton(idx3));

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx4));
    EsApiIntegration.assertSearchSettings(idx4);
  }

  @Test
  public void swapMissingIndexTest() {

    String i = "swap-missing-index-test";
    String alias = ALIAS_FN.apply(i);

    thrown.expect(ResponseException.class);
    thrown.expectMessage(
        CoreMatchers.containsString("URI [/_aliases], status line [HTTP/1.1 404 Not Found"));

    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias), "dummy_1");
  }

  @Test
  public void countIndexDocumentsAfterSwappingTest() throws InterruptedException {

    String id = "count-index-documents-after-swapping-test";
    String idxName = IDX_NAME_FN.apply(id);
    String alias = ALIAS_FN.apply(id);

    // create index
    String idx =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder()
                .indexName(idxName)
                .pathMappings(EsApiIntegration.TEST_MAPPINGS_PATH)
                .build());

    // index some documents
    long n = 3;
    String document = "{\"test\" : \"test value\"}";
    for (int i = 1; i <= n; i++) {
      EsService.indexDocument(EsApiIntegration.ES_SERVER.getEsClient(), idx, i, document);
    }

    // swap index in alias
    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias), idx);

    // wait the refresh interval for the documents to become searchable.
    TimeUnit.MILLISECONDS.sleep(
        (Long.parseLong(Iterables.get(Splitter.on('s').split(Searching.REFRESH_INTERVAL), 0)) * 1000
            + 500));

    // assert results against the alias
    assertEquals(n, EsIndex.countDocuments(EsApiIntegration.ES_SERVER.getEsConfig(), alias));
    // assert results against the index
    assertEquals(n, EsIndex.countDocuments(EsApiIntegration.ES_SERVER.getEsConfig(), idx));
  }

  /** Utility method to assert a newly created index. */
  private static void assertIndexWithSettingsAndIndexName(String idxName, String idxCreated) {
    // assert index created
    assertTrue(EsService.existsIndex(EsApiIntegration.ES_SERVER.getEsClient(), idxCreated));
    // assert index settings
    EsApiIntegration.assertIndexingSettings(idxCreated);
    // assert idx name
    assertEquals(idxName + INDEX_SEPARATOR + DEFAULT_ATTEMPT, idxCreated);
  }

  @Test
  public void findDatasetIndexesInAliasTest() {

    String id = "find-dataset-indexes-in-alias-test";

    // create index
    final String datasetKey = "82ceb6ba-f762-11e1-a439-00145eb45e9a";
    String idx1 =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().mappings(TEST_MAPPINGS).datasetKey(id).attempt(1).build());

    // index some documents
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";
    EsService.indexDocument(EsApiIntegration.ES_SERVER.getEsClient(), idx1, 1, document);
    EsService.refreshIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx1);

    // add index to alias
    final String alias = "alias1";
    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias), idx1);

    // When
    Set<String> indexesFound =
        EsIndex.findDatasetIndexesInAliases(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            new String[] {alias, "fakeAlias"},
            datasetKey);

    // Should
    assertEquals(1, indexesFound.size());
    assertTrue(indexesFound.contains(idx1));
  }

  @Test
  public void findDatasetIndexesInAliasesTest() {

    String id = "find-dataset-indexes-in-aliases-test";

    // create index
    final String datasetKey = "6aae644c-59e7-4f9e-9622-2ee9835cb689";
    String idx1 =
        EsIndex.createIndex(
            EsApiIntegration.ES_SERVER.getEsConfig(),
            IndexParams.builder().mappings(TEST_MAPPINGS).datasetKey(id).attempt(1).build());

    // index some documents
    String document = "{\"datasetKey\" : \"" + datasetKey + "\"}";
    EsService.indexDocument(EsApiIntegration.ES_SERVER.getEsClient(), idx1, 1, document);
    EsService.refreshIndex(EsApiIntegration.ES_SERVER.getEsClient(), idx1);

    // add index to aliases
    final String alias1 = "alias10";
    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias1), idx1);

    final String alias2 = "alias20";
    EsIndex.swapIndexInAliases(
        EsApiIntegration.ES_SERVER.getEsConfig(), Collections.singleton(alias2), idx1);

    // When
    Set<String> indexesFound =
        EsIndex.findDatasetIndexesInAliases(
            EsApiIntegration.ES_SERVER.getEsConfig(), new String[] {alias1, alias2}, datasetKey);

    // Should
    assertEquals(1, indexesFound.size());
    assertTrue(indexesFound.contains(idx1));
  }
}
