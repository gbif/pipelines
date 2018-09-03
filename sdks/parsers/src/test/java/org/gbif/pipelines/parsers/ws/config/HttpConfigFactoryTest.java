package org.gbif.pipelines.parsers.ws.config;

import java.nio.file.Paths;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.gbif.pipelines.parsers.ws.config.WsConfigFactory.DEFAULT_CACHE_SIZE;
import static org.gbif.pipelines.parsers.ws.config.WsConfigFactory.DEFAULT_TIMEOUT;

/** Tests the {@link WsConfigFactory}. */
public class HttpConfigFactoryTest {

  // this file has the geocode properties wrong on purpose
  private static final String TEST_PROPERTIES_FILE = "ws-test.properties";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void speciesMatch2ConfiguratorTest() {
    WsConfig config =
        WsConfigFactory.create(ServiceType.SPECIES_MATCH2, Paths.get(TEST_PROPERTIES_FILE));

    Assert.assertNotNull(config);
    // defaults apply
    Assert.assertEquals(DEFAULT_TIMEOUT, config.getTimeout());
    Assert.assertEquals(100L * 1024L * 1024L, config.getCacheSize());
  }

  @Test
  public void createConfigFromUrlTest() {
    String url = "http://localhost";
    WsConfig config = WsConfigFactory.createFromUrl(url);

    Assert.assertNotNull(config);
    Assert.assertEquals(url, config.getBasePath());
    // defaults apply
    Assert.assertEquals(DEFAULT_TIMEOUT, config.getTimeout());
    Assert.assertEquals(DEFAULT_CACHE_SIZE, config.getCacheSize());
  }

  @Test
  public void givenWrongConfigurationWhenGettingConfigThenExceptionThrownl() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("WS base path is required");

    WsConfigFactory.create(ServiceType.GEO_CODE, Paths.get(TEST_PROPERTIES_FILE));
  }

  @Test
  public void givenWrongPropertiesPathWhenGettingConfigThenExceptionThrown() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(CoreMatchers.containsString("Could not load properties file"));

    WsConfigFactory.create(ServiceType.GEO_CODE, Paths.get("unknown"));
  }

  @Test
  public void givenNullServiceWhenGettingConfigThenExceptionThrown() {
    thrown.expect(NullPointerException.class);

    WsConfigFactory.create(null, Paths.get("unknown"));
  }
}
