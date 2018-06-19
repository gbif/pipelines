package org.gbif.pipelines.core.ws.config;

import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import static org.gbif.pipelines.core.ws.config.HttpConfigFactory.DEFAULT_CACHE_SIZE;
import static org.gbif.pipelines.core.ws.config.HttpConfigFactory.DEFAULT_TIMEOUT;

/**
 * Tests the {@link HttpConfigFactory}.
 */
public class HttpConfigFactoryTest {

  // this file has the geocode properties wrong on purpose
  private static final String TEST_PROPERTIES_FILE = "ws-test.properties";

  @Test
  public void speciesMatch2ConfiguratorTest() {
    Config config = HttpConfigFactory.createConfig(Service.SPECIES_MATCH2, Paths.get(TEST_PROPERTIES_FILE));

    Assert.assertNotNull(config);
    // defaults apply
    Assert.assertEquals(DEFAULT_TIMEOUT, config.getTimeout());
    Assert.assertEquals(100L * 1024L * 1024L, config.getCacheSize());
  }

  @Test
  public void createConfigFromUrlTest() {
    String url = "http://localhost";
    Config config = HttpConfigFactory.createConfigFromUrl(url);

    Assert.assertNotNull(config);
    Assert.assertEquals(url, config.getBasePath());
    // defaults apply
    Assert.assertEquals(DEFAULT_TIMEOUT, config.getTimeout());
    Assert.assertEquals(DEFAULT_CACHE_SIZE, config.getCacheSize());
  }

  @Test(expected = IllegalArgumentException.class)
  public void givenWrongConfigurationWhenGettingConfigThenExceptionThrownl() {
    HttpConfigFactory.createConfig(Service.GEO_CODE, Paths.get(TEST_PROPERTIES_FILE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void givenWrongPropertiesPathWhenGettingConfigThenExceptionThrown() {
    HttpConfigFactory.createConfig(Service.GEO_CODE, Paths.get("unknown"));
  }

  @Test(expected = NullPointerException.class)
  public void givenNullServiceWhenGettingConfigThenExceptionThrown() {
    HttpConfigFactory.createConfig(null, Paths.get("unknown"));
  }

}
