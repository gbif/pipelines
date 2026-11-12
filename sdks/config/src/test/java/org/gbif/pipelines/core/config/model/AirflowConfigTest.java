package org.gbif.pipelines.core.config.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AirflowConfigTest {
  @Test
  void testCredentialExludedTostring() {
    AirflowConfig airflowConfig = new AirflowConfig();

    String password = "SuperSecretPassword";
    String username = "YouWillNeverFindOutMyUsername";
    airflowConfig.pass = password;
    airflowConfig.user = username;

    String address = "someUrl.url";
    airflowConfig.address = address;

    String toString = airflowConfig.toString();

    // Not to contain (Sensitive)
    assertFalse(toString.contains(password));
    assertFalse(toString.contains(username));
    assertFalse(toString.contains("pass"));
    assertFalse(toString.contains("user"));

    // Fine to contain
    assertTrue(toString.contains(address));
    assertTrue(toString.contains("address"));
  }
}
