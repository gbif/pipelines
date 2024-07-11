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
    int attempt = 1_000_000;

    // When
    String string = AppName.get(dwca, uuid, attempt);

    // Should
    Assert.assertEquals("dwca-verba-ad2ef207-969e-418a-ab4f-102e8d9bf7ac-1000000", string);
  }

  @Test
  public void getOccurrenceTest() {
    // State
    StepType dwca = StepType.VERBATIM_TO_INTERPRETED;
    UUID uuid = UUID.fromString("ad2ef207-969e-418a-ab4f-102e8d9bf7ac");
    int attempt = 1_000_000;

    // When
    String string = AppName.get(dwca, uuid, attempt);

    // Should
    Assert.assertEquals("verba-inter-ad2ef207-969e-418a-ab4f-102e8d9bf7ac-1000000", string);
  }

  @Test
  public void getEventTest() {
    // State
    StepType dwca = StepType.EVENTS_VERBATIM_TO_INTERPRETED;
    UUID uuid = UUID.fromString("ad2ef207-969e-418a-ab4f-102e8d9bf7ac");
    int attempt = 1_000_000;

    // When
    String string = AppName.get(dwca, uuid, attempt);

    // Should
    Assert.assertEquals("event-verba-inter-ad2ef207-969e-418a-ab4f-102e8d9bf7ac-1000000", string);
  }
}
