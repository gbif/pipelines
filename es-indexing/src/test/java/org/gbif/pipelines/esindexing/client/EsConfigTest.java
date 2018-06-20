package org.gbif.pipelines.esindexing.client;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link EsConfig}. */
public class EsConfigTest {

  private static final String DUMMY_HOST = "http://dummy.com";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createConfigTest() {
    EsConfig config = EsConfig.from(DUMMY_HOST);
    assertEquals(1, config.getHosts().size());
    assertEquals(DUMMY_HOST, config.getHosts().get(0).toString());
  }

  @Test
  public void createConfigNullHostsTest() {
    thrown.expect(NullPointerException.class);
    EsConfig.from((String[]) null);
  }

  @Test
  public void createConfigEmptyHostsTest() {
    EsConfig config = EsConfig.from();
    assertTrue(config.getHosts().isEmpty());
  }

  @Test
  public void createConfigInvalidHostsTest() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(CoreMatchers.containsString("is not a valid url"));
    EsConfig.from("wrong url");
  }
}
