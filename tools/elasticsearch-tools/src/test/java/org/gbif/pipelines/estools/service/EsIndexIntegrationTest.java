package org.gbif.pipelines.estools.service;

import org.gbif.pipelines.estools.EsIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.gbif.pipelines.estools.EsIndex.INDEX_SEPARATOR;
import static org.gbif.pipelines.estools.service.EsConstants.Field;
import static org.gbif.pipelines.estools.service.EsConstants.Searching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link EsIndex}. */
public class EsIndexIntegrationTest extends EsApiIntegration {

  private static final String DATASET_TEST = "abc";
  private static final String ALIAS_TEST = "alias";
  private static final int DEFAULT_ATTEMPT = 1;
  private static final String SEARCH = DATASET_TEST + INDEX_SEPARATOR + "*";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @After
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @Test
  public void createIndexTest() {
    // create index
    String idxCreated = EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, DEFAULT_ATTEMPT);

    // assert index created
    assertIndexWithSettingsAndIndexName(idxCreated, DATASET_TEST, DEFAULT_ATTEMPT);
  }

  @Test
  public void createIndexWithMappingsTest() {
    // create index
    String idxCreated =
        EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, DEFAULT_ATTEMPT, TEST_MAPPINGS_PATH);

    // assert index created
    assertIndexWithSettingsAndIndexName(idxCreated, DATASET_TEST, DEFAULT_ATTEMPT);

    // assert mappings
    JsonNode mappings = getMappingsFromIndex(idxCreated).path(idxCreated).path(Field.MAPPINGS);
    assertTrue(mappings.has("doc"));
  }

  @Test
  public void swpaIndexInEmptyAliasTest() {
    // create index
    String idxCreated = EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, 1);

    // swap index
    EsIndex.swapIndexInAlias(ES_SERVER.getEsConfig(), ALIAS_TEST, idxCreated);

    // assert result
    assertSwapResults(idxCreated, SEARCH, ALIAS_TEST, Collections.emptySet());

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxCreated));
    assertSearchSettings(idxCreated);
  }

  @Test
  public void swapIndexInAliasTest() {
    // create index
    String idx1 = EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, 1);
    String idx2 = EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, 2);
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2));

    // add the indexes to the alias
    addIndexesToAlias(ALIAS_TEST, initialIndexes);

    // create another index and swap it in the alias
    String idx3 = EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, 3);
    EsIndex.swapIndexInAlias(ES_SERVER.getEsConfig(), ALIAS_TEST, idx3);

    // alias should have only the last index created
    assertSwapResults(idx3, SEARCH, ALIAS_TEST, initialIndexes);

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx3));
    assertSearchSettings(idx3);

    // create another index and swap it again
    String idx4 = EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, 4);
    EsIndex.swapIndexInAlias(ES_SERVER.getEsConfig(), ALIAS_TEST, idx4);

    // alias should have only the last index created
    assertSwapResults(idx4, SEARCH, ALIAS_TEST, Collections.singleton(idx3));

    // check settings of index after swapping
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idx4));
    assertSearchSettings(idx4);
  }

  @Test
  public void swapMissingIndexTest() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(CoreMatchers.containsString("Error swapping index"));

    EsIndex.swapIndexInAlias(ES_SERVER.getEsConfig(), ALIAS_TEST, "dummy_1");
  }

  @Test
  public void countIndexDocumentsAfterSwappingTest() throws InterruptedException {
    // create index
    String idx =
        EsIndex.create(ES_SERVER.getEsConfig(), DATASET_TEST, DEFAULT_ATTEMPT, TEST_MAPPINGS_PATH);

    // index some documents
    long n = 3;
    final String type = "doc";
    String document = "{\"test\" : \"test value\"}";
    for (int i = 1; i <= n; i++) {
      EsService.indexDocument(ES_SERVER.getEsClient(), idx, type, i, document);
    }

    // swap index in alias
    EsIndex.swapIndexInAlias(ES_SERVER.getEsConfig(), ALIAS_TEST, idx);

    // wait the refresh interval for the documents to become searchable.
    Thread.sleep(
        Long.valueOf(Iterables.get(Splitter.on('s').split(Searching.REFRESH_INTERVAL), 0)) * 1000
            + 500);

    // assert results against the alias
    assertEquals(n, EsIndex.countDocuments(ES_SERVER.getEsConfig(), ALIAS_TEST));
    // assert results against the index
    assertEquals(n, EsIndex.countDocuments(ES_SERVER.getEsConfig(), idx));
  }

  /** Utility mehtod to assert a newly created index. */
  private static void assertIndexWithSettingsAndIndexName(
      String idxCreated, String datasetId, int attempt) {
    // assert index created
    assertTrue(EsService.existsIndex(ES_SERVER.getEsClient(), idxCreated));
    // assert index settings
    assertIndexingSettings(idxCreated);
    // assert idx name
    assertEquals(datasetId + INDEX_SEPARATOR + attempt, idxCreated);
  }
}
