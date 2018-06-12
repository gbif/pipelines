package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.EsIntegrationTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.client.Response;
import org.junit.After;
import org.junit.Test;

import static org.gbif.pipelines.esindexing.common.SettingsType.INDEXING;
import static org.gbif.pipelines.esindexing.common.SettingsType.SEARCH;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link EsService}.
 */
public class EsServiceIT extends EsIntegrationTest {

  private static final String ALIAS_TEST = "alias";

  @After
  public void cleanIndexes() {
    deleteAllIndexes();
  }

  @Test
  public void createAndUpdateIndexWithSettingsTest() {
    String idx = EsService.createIndexWithSettings(getEsClient(), "idx-settings", INDEXING);

    // check that the index was created as expected
    Response response = assertCreatedIndex(idx);

    // check settings
    assertIndexingSettings(response, idx);

    EsService.updateIndexSettings(getEsClient(), idx, SEARCH);

    // check that the index was updated as expected
    response = assertCreatedIndex(idx);

    // check settings
    assertSearchSettings(response, idx);
  }

  @Test(expected = IllegalStateException.class)
  public void updateMissingIndex() {
    EsService.updateIndexSettings(getEsClient(), "fake-index", INDEXING);
  }

  @Test(expected = IllegalStateException.class)
  public void createWrongIndexTest() {
    EsService.createIndexWithSettings(getEsClient(), "UPPERCASE", INDEXING);
  }

  @Test
  public void getIndexesByAliasAndSwapIndexTest() {
    // create some indexes to test
    String idx1 = EsService.createIndexWithSettings(getEsClient(), "idx1", INDEXING);
    String idx2 = EsService.createIndexWithSettings(getEsClient(), "idx2", INDEXING);
    String idx3 = EsService.createIndexWithSettings(getEsClient(), "idx3", INDEXING);
    Set<String> initialIndexes = new HashSet<>(Arrays.asList(idx1, idx2, idx3));

    // there shouldn't be indexes before we start
    Set<String> indexes = EsService.getIndexesByAliasAndIndexPattern(getEsClient(), "idx*", ALIAS_TEST);
    assertEquals(0, indexes.size());

    // add them to the same alias
    addIndexToAlias(ALIAS_TEST, initialIndexes);

    // get the indexes of the alias
    indexes = EsService.getIndexesByAliasAndIndexPattern(getEsClient(), "idx*", ALIAS_TEST);

    // assert conditions
    assertEquals(3, indexes.size());
    assertTrue(indexes.containsAll(initialIndexes));

    // create a new index and swap it to the alias
    String idx4 = EsService.createIndexWithSettings(getEsClient(), "idx4", INDEXING);
    EsService.swapIndexes(getEsClient(), ALIAS_TEST, Collections.singleton(idx4), initialIndexes);
    assertSwapResults(idx4, "idx*", ALIAS_TEST, initialIndexes);

    // repeat previous step with a new index
    String idx5 = EsService.createIndexWithSettings(getEsClient(), "idx5", INDEXING);
    EsService.swapIndexes(getEsClient(), ALIAS_TEST, Collections.singleton(idx5), Collections.singleton(idx4));
    assertSwapResults(idx5, "idx*", ALIAS_TEST, initialIndexes);
  }

  @Test
  public void getIndexesFromMissingAlias() {
    Set<String> idx = EsService.getIndexesByAliasAndIndexPattern(getEsClient(), "idx*", "fake-alias");
    assertTrue(idx.isEmpty());
  }

  @Test
  public void swapEmptyAliasTest() {
    String idx1 = EsService.createIndexWithSettings(getEsClient(), "idx1", INDEXING);
    EsService.swapIndexes(getEsClient(), ALIAS_TEST, Collections.singleton(idx1), Collections.emptySet());
    assertSwapResults(idx1, "idx*", ALIAS_TEST, Collections.emptySet());
  }

  @Test(expected = IllegalStateException.class)
  public void swapMissingIndexTest() {
    EsService.swapIndexes(getEsClient(), "fake-alias", Collections.singleton("fake-index"), Collections.emptySet());
  }
}
