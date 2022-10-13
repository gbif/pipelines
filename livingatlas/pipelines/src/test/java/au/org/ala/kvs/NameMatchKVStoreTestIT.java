package au.org.ala.kvs;

import static au.org.ala.util.TestUtils.NAME_SERVICE_IMG;
import static au.org.ala.util.TestUtils.NAME_SERVICE_INTERNAL_PORT;
import static org.junit.Assert.assertNotNull;

import au.org.ala.kvs.cache.ALANameMatchKVStoreFactory;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.util.TestUtils;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class NameMatchKVStoreTestIT {

  @Rule
  public GenericContainer nameService =
      new GenericContainer(DockerImageName.parse(NAME_SERVICE_IMG))
          .withExposedPorts(NAME_SERVICE_INTERNAL_PORT);

  @Before
  public void setup() throws Exception {
    TestUtils.setNameServicePort(nameService.getMappedPort(NAME_SERVICE_INTERNAL_PORT));
  }

  /**
   * Tests the Get operation on {@link KeyValueCache} that wraps a simple KV store backed by a
   * HashMap.
   */
  @Test
  public void getCacheTest() throws Exception {

    KeyValueStore<NameSearch, NameUsageMatch> kvs =
        ALANameMatchKVStoreFactory.create(TestUtils.getConfig());
    NameSearch req = NameSearch.builder().scientificName("Macropus rufus").build();
    NameUsageMatch match = kvs.get(req);
    assertNotNull(match.getTaxonConceptID());

    NameSearch req2 = NameSearch.builder().scientificName("Osphranter rufus").build();
    NameUsageMatch match2 = kvs.get(req2);
    assertNotNull(match2.getTaxonConceptID());
    kvs.close();
  }
}
