package au.org.ala.kvs;

import static org.junit.Assert.*;

import au.org.ala.kvs.cache.SDSReportKVStoreFactory;
import au.org.ala.sds.api.SensitivityInstance;
import au.org.ala.sds.api.SensitivityQuery;
import au.org.ala.sds.api.SensitivityReport;
import au.org.ala.util.TestUtils;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SDSReportKVStoreTestIT {
  private KeyValueStore<SensitivityQuery, SensitivityReport> kvs;

  @Before
  public void setUp() throws Exception {
    this.kvs = SDSReportKVStoreFactory.create(TestUtils.getConfig());
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
  public void getCacheTest1() throws Exception {
    SensitivityQuery lookup =
        SensitivityQuery.builder()
            .scientificName("Amanita walpolei")
            .stateProvince("Western Australia")
            .build();
    SensitivityReport result = this.kvs.get(lookup);
    assertNotNull(result);
    assertTrue(result.isSensitive());
    assertNull(result.getReport().getCategory());
    assertNotNull(result.getReport().getTaxon());
    assertNotNull(result.getReport().getTaxon().getInstances());
    assertEquals(1, result.getReport().getTaxon().getInstances().size());
    assertEquals(
        SensitivityInstance.SensitivityType.CONSERVATION,
        result.getReport().getTaxon().getInstances().get(0).getType());
  }

  /** Test for an invalid response */
  @Test
  public void getCacheTest2() throws Exception {
    SensitivityQuery lookup =
        SensitivityQuery.builder()
            .scientificName("Osphranter rufus")
            .stateProvince("New South Wales")
            .build();
    SensitivityReport result = this.kvs.get(lookup);
    assertNotNull(result);
    assertFalse(result.isSensitive());
  }
}
