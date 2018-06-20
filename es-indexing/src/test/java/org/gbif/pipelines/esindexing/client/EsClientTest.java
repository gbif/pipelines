package org.gbif.pipelines.esindexing.client;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link EsClient}. */
public class EsClientTest {

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createClientFromNullConfigTest() {
    thrown.expect(NullPointerException.class);
    EsClient.from(null);
  }

  @Test
  public void createClientFromEmptyHostsTest() {
    thrown.expect(IllegalArgumentException.class);
    EsClient.from(EsConfig.from());
  }
}
