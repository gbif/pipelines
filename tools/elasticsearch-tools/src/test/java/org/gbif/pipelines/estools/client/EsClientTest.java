package org.gbif.pipelines.estools.client;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link EsClient}. */
public class EsClientTest {

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test(expected = NullPointerException.class)
  public void createClientFromNullConfigTest() {

    // When
    EsClient.from(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createClientFromEmptyHostsTest() {

    // When
    EsClient.from(EsConfig.from());
  }
}
