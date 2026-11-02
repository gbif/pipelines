package org.gbif.pipelines.coordinator;

import static org.junit.jupiter.api.Assertions.*;

import org.gbif.pipelines.coordinator.Coordinator.Mode;
import org.junit.jupiter.api.Test;

class CoordinatorTest {

  @Test
  void testMode() {
    Mode result = Mode.valueOf("VALIDATOR_DWCDP_TO_VERBATIM_DISTRIBUTED");
    assertEquals(Mode.VALIDATOR_DWCDP_TO_VERBATIM_DISTRIBUTED, result);
  }
}
