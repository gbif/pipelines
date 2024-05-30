package org.gbif.pipelines.common.airflow;

import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.junit.Assert;
import org.junit.Test;

public class AppNameTest {

  @Test
  public void getTest() {
    // State
    StepType dwca = StepType.DWCA_TO_VERBATIM;
    UUID uuid = UUID.fromString("ad2ef207-969e-418a-ab4f-102e8d9bf7ac");

    // When
    String string = AppName.get(dwca, uuid);

    // Should
    Assert.assertEquals("DWCA_TO_VERBATIM_ad2ef207-969e-418a-ab4f-102e8d9bf7ac", string);
  }
}
