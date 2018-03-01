package org.gbif.pipelines.http.config;

import org.gbif.pipelines.http.HttpConfigFactory;

import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link HttpConfigFactory}.
 */
public class HttpConfigFactoryTest {

  private static final String TEST_PROPERTIES_FILE = "ws-test.properties";

  @Test
  public void speciesMatch2ConfiguratorTest() {
    Config config = HttpConfigFactory.createConfig(Service.SPECIES_MATCH2, Paths.get(TEST_PROPERTIES_FILE));

    Assert.assertNotNull(config);
    // default timeout applies
    Assert.assertEquals(60, config.getTimeout());
    Assert.assertNotNull(config.getCacheConfig());
    Assert.assertEquals(Service.SPECIES_MATCH2.name().toLowerCase().concat("-cacheWs"),
                        config.getCacheConfig().getName());
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
