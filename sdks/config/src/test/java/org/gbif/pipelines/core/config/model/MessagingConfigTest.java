package org.gbif.pipelines.core.config.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class MessagingConfigTest {
  @Test
  void testCredentialExcludedToString() {
    MessagingConfig messagingConfig = new MessagingConfig();

    String password = "SuperSecretPassword";
    String username = "YouWillNeverFindOutMyUsername";
    messagingConfig.password = password;
    messagingConfig.username = username;

    String host = "rabbit.gbif.org";
    String virtualHost = "/gbif";
    messagingConfig.host = host;
    messagingConfig.port = 5672;
    messagingConfig.virtualHost = virtualHost;
    messagingConfig.prefetchCount = 10;

    String toString = messagingConfig.toString();

    // Not to contain (Sensitive)
    assertFalse(toString.contains(password));
    assertFalse(toString.contains(username));
    assertFalse(toString.contains("password"));
    assertFalse(toString.contains("username"));

    // Fine to contain
    assertTrue(toString.contains("host=" + host));
    assertTrue(toString.contains("port=5672"));
    assertTrue(toString.contains("virtualHost=" + virtualHost));
    assertTrue(toString.contains("prefetchCount=10"));
  }
}
