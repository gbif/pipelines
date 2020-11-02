package org.gbif.pipelines.estools.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link EsConfig}. */
public class EsConfigTest {

  private static final String DUMMY_HOST = "http://dummy.com";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createConfigTest() {

    // When
    EsConfig config = EsConfig.from(DUMMY_HOST);

    // Should
    assertEquals(1, config.getHosts().size());
    assertEquals(DUMMY_HOST, config.getHosts().get(0).toString());
  }

  @Test(expected = NullPointerException.class)
  public void createConfigNullHostsTest() {

    // When
    EsConfig.from((String[]) null);
  }

  @Test
  public void createConfigEmptyHostsTest() {

    // When
    EsConfig config = EsConfig.from();

    // Should
    assertTrue(config.getHosts().isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void createConfigInvalidHostsTest() {

    // When
    EsConfig.from("wrong url");

    // Should
    thrown.expectMessage(CoreMatchers.containsString("is not a valid url"));
  }
}
