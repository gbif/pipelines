package org.gbif.pipelines.ws.config;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link WsConfiguration}.
 */
public class WsConfigurationTest {

  private static final String TEST_PROPERTIES_FILE = "ws-test.properties";

  @Test
  public void speciesMatch2ConfiguratorTest() {
    Config config =
      WsConfiguration.of(Service.SPECIES_MATCH2).fromProperties(TEST_PROPERTIES_FILE).getConfigOrThrowException();

    Assert.assertNotNull(config);
    // default timeout applies
    Assert.assertEquals(60, config.getTimeout());
    Assert.assertNotNull(config.getCacheConfig());
    Assert.assertEquals("match2-test-cache", config.getCacheConfig().getName());
  }

  @Test
  public void givenWrongConfigurationWhenGettingConfigThenReturnNull() {
    Optional<Config> optConfig = WsConfiguration.of(Service.GEO_CODE).fromProperties(TEST_PROPERTIES_FILE).getConfig();

    Assert.assertFalse(optConfig.isPresent());
  }

  @Test(expected = IllegalArgumentException.class)
  public void givenWrongConfigurationWhenGettingConfigOrThrowExceptionThenExceptionThrown() {
    WsConfiguration.of(Service.GEO_CODE).fromProperties(TEST_PROPERTIES_FILE).getConfigOrThrowException();
  }

  @Test(expected = IllegalArgumentException.class)
  public void givenWrongPropertiesPathWhenGettingConfigOrThrowExceptionThenExceptionThrown() {
    WsConfiguration.of(Service.GEO_CODE).fromProperties("unknown.properties").getConfigOrThrowException();
  }

  @Test(expected = NullPointerException.class)
  public void givenNullServiceWhenGettingConfigThenNullPointer() {
    WsConfiguration.of(null).fromProperties("unknown.properties").getConfigOrThrowException();
  }

}
