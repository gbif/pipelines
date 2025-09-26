package au.org.ala.kvs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import au.org.ala.kvs.cache.ALANameCheckKVStoreFactory;
import au.org.ala.util.IntegrationTestUtils;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.junit.ClassRule;
import org.junit.Test;

public class NameCheckKVStoreTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  /**
   * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void getCacheTest1() throws Exception {
    KeyValueStore<String, Boolean> kvs =
        ALANameCheckKVStoreFactory.create("kingdom", itUtils.getConfig());
    Boolean match = kvs.get("Animalia");
    assertNotNull(match);
    assertTrue(match);
    kvs.close();
  }

  /** Test for an invalid response */
  @Test
  public void getCacheTest2() throws Exception {
    KeyValueStore<String, Boolean> kvs =
        ALANameCheckKVStoreFactory.create("kingdom", itUtils.getConfig());
    Boolean match = kvs.get("Never in your life");
    assertNotNull(match);
    assertFalse(match);
    kvs.close();
  }

  /** Test for a null response (homonym in this case) */
  @Test
  public void getCacheTest3() throws Exception {
    KeyValueStore<String, Boolean> kvs =
        ALANameCheckKVStoreFactory.create("genus", itUtils.getConfig());
    Boolean match = kvs.get("Macropus");
    assertNull(match);
    kvs.close();
  }
}
