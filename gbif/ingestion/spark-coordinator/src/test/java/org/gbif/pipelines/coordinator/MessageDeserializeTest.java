package org.gbif.pipelines.coordinator;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import java.util.UUID;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.junit.Test;

public class MessageDeserializeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testDeserializeDwcDpStageMessage() throws Exception {
    String json =
        "{\"datasetUuid\":\"2d02d951-ff41-44d5-b018-53604e145603\",\"attempt\":42,\"pipelineSteps\":[\"dwcdpStage\"],\"executionId\":0}";

    DwcDpStageMessage message = MAPPER.readValue(json, DwcDpStageMessage.class);

    assertEquals(UUID.fromString("2d02d951-ff41-44d5-b018-53604e145603"), message.getDatasetUuid());
    assertEquals(42, (int) message.getAttempt());
    assertEquals(Set.of("dwcdpStage"), message.getPipelineSteps());
    assertEquals(0L, (long) message.getExecutionId());
  }
}
