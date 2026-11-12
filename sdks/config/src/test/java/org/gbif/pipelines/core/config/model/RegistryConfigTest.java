package org.gbif.pipelines.core.config.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RegistryConfigTest {
  @Test
  void testCredentialExcludedToString() {
    RegistryConfig registryConfig = new RegistryConfig();

    String password = "SuperSecretPassword";
    String username = "YouWillNeverFindOutMyUsername";
    registryConfig.password = password;
    registryConfig.user = username;

    String wsUrl = "https://api.gbif.org/v1/";
    registryConfig.wsUrl = wsUrl;

    String toString = registryConfig.toString();

    // Not to contain (Sensitive)
    assertFalse(toString.contains(password));
    assertFalse(toString.contains(username));
    assertFalse(toString.contains("password"));
    assertFalse(toString.contains("user"));

    // Fine to contain
    assertTrue(toString.contains(wsUrl));
    assertTrue(toString.contains("wsUrl"));
  }
}
