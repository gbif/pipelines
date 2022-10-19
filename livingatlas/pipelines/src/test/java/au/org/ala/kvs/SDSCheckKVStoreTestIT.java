package au.org.ala.kvs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import au.org.ala.kvs.cache.SDSCheckKVStoreFactory;
import au.org.ala.sds.api.SpeciesCheck;
import au.org.ala.util.IntegrationTestUtils;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class SDSCheckKVStoreTestIT {
  private KeyValueStore<SpeciesCheck, Boolean> kvs;

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  @Before
  public void setup() throws Exception {
    this.kvs = SDSCheckKVStoreFactory.create(itUtils.getConfig());
  }

  @After
  public void tearDown() throws Exception {
    if (this.kvs != null) this.kvs.close();
  }

  /**
   * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void getCacheTest1() {
    SpeciesCheck lookup = SpeciesCheck.builder().scientificName("Amanita walpolei").build();
    Boolean result = this.kvs.get(lookup);
    assertNotNull(result);
    assertTrue(result);
  }

  /** Test for an invalid response */
  @Test
  public void getCacheTest2() {
    SpeciesCheck lookup = SpeciesCheck.builder().scientificName("Osphranter rufus").build();
    Boolean result = this.kvs.get(lookup);
    assertNotNull(result);
    assertFalse(result);
  }
}
