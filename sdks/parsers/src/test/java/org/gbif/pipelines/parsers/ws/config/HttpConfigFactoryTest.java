package org.gbif.pipelines.parsers.ws.config;

import org.gbif.pipelines.parsers.exception.IORuntimeException;

import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the {@link WsConfigFactory}. */
public class HttpConfigFactoryTest {

  // this file has the geocode properties wrong on purpose
  private static final String TEST_PROPERTIES_FILE = "ws-test.properties";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void speciesMatch2ConfiguratorTest() {
    WsConfig config = WsConfigFactory.create("match", Paths.get(TEST_PROPERTIES_FILE));

    Assert.assertNotNull(config);
    // defaults apply
    Assert.assertTrue(config.getTimeout() > 0);
    Assert.assertTrue(config.getCacheSize() > 0);
  }

  @Test
  public void createConfigFromUrlTest() {
    String url = "http://localhost";
    WsConfig config = WsConfigFactory.create(url);

    Assert.assertNotNull(config);
    Assert.assertEquals(url, config.getBasePath());
    // defaults apply
    Assert.assertTrue(config.getTimeout() > 0);
    Assert.assertTrue(config.getCacheSize() > 0);
  }

  @Test(expected = IORuntimeException.class)
  public void givenWrongPropertiesPathWhenGettingConfigThenExceptionThrown() {
    WsConfigFactory.create("match", Paths.get("unknown"));
  }

  @Test(expected = NullPointerException.class)
  public void givenNullServiceWhenGettingConfigThenExceptionThrown() {
    WsConfigFactory.create(null, Paths.get("unknown"));
  }
}
