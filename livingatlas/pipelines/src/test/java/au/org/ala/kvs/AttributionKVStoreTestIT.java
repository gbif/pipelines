package au.org.ala.kvs;

import static org.junit.Assert.*;

import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.cache.ALACollectionKVStoreFactory;
import au.org.ala.kvs.client.ALACollectionLookup;
import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.kvs.client.ConnectionParameters;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.util.TestUtils;
import org.gbif.kvs.KeyValueStore;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for Attribution KV store */
public class AttributionKVStoreTestIT {

  IntegrationTestUtils itUtils;

  @Before
  public void setup() throws Exception {
    // clear up previous test runs
    itUtils = IntegrationTestUtils.getInstance();
    itUtils.setup();
  }

  @Test
  public void testAttributionHubMembership() throws Exception {
    KeyValueStore<String, ALACollectoryMetadata> kvs =
        ALAAttributionKVStoreFactory.create(TestUtils.getConfig());
    ALACollectoryMetadata m = kvs.get("dr376");
    assertEquals(2, m.getHubMembership().size());

    kvs.close();
  }

  @Test
  public void testAttributionLookup() throws Exception {

    KeyValueStore<String, ALACollectoryMetadata> kvs =
        ALAAttributionKVStoreFactory.create(TestUtils.getConfig());
    ALACollectoryMetadata m = kvs.get("dr893");
    ConnectionParameters connParams = m.getConnectionParameters();

    assertNotNull(m.getName());
    assertNotNull(connParams);
    assertNotNull(connParams.getUrl());
    assertNotNull(connParams.getTermsForUniqueKey());
    assertFalse(connParams.getTermsForUniqueKey().isEmpty());
    assertNotNull(m.getDefaultDarwinCoreValues());
    assertFalse(m.getDefaultDarwinCoreValues().isEmpty());
    assertNotNull(m.getProvenance());
    assertNotNull(m.getTaxonomyCoverageHints());
    assertEquals(0, m.getTaxonomyCoverageHints().size());

    kvs.close();
  }

  @Test
  public void testAttributionLookupMultiUrl() throws Exception {

    KeyValueStore<String, ALACollectoryMetadata> kvs =
        ALAAttributionKVStoreFactory.create(TestUtils.getConfig());
    ALACollectoryMetadata m = kvs.get("dr807");
    ConnectionParameters connParams = m.getConnectionParameters();

    assertNotNull(m.getName());
    assertNotNull(connParams);
    assertNotNull(connParams.getUrl());
    kvs.close();
  }

  @Test
  public void testAttributionConnectionIssues() throws Exception {
    KeyValueStore<String, ALACollectoryMetadata> kvs =
        ALAAttributionKVStoreFactory.create(TestUtils.getConfig());
    ALACollectoryMetadata m = kvs.get("dr893XXXXXX");
    assertEquals(m, ALACollectoryMetadata.EMPTY);
    kvs.close();
  }

  @Test
  public void testCollectionLookup() {

    KeyValueStore<ALACollectionLookup, ALACollectionMatch> kvs =
        ALACollectionKVStoreFactory.create(TestUtils.getConfig());
    ALACollectionLookup lookup =
        ALACollectionLookup.builder().institutionCode("CSIRO").collectionCode("ANIC").build();
    ALACollectionMatch m = kvs.get(lookup);
    assertNotNull(m.getCollectionUid());
    assertEquals("co13", m.getCollectionUid());
  }

  @Test
  public void testCollectionLookupFail() throws Exception {

    KeyValueStore<ALACollectionLookup, ALACollectionMatch> kvs =
        ALACollectionKVStoreFactory.create(TestUtils.getConfig());
    ALACollectionLookup lookup =
        ALACollectionLookup.builder().institutionCode("CSIROCXXX").collectionCode("ANIC").build();
    ALACollectionMatch m = kvs.get(lookup);
    assertNull(m.getCollectionUid());
    assertEquals(ALACollectionMatch.EMPTY, m);
    kvs.close();
  }
}
