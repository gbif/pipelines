package org.gbif.pipelines.coordinator;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import java.util.UUID;
import org.gbif.common.messaging.api.messages.DwcDpNfsToHdfsMessage;
import org.junit.Test;

public class MessageDeserializeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testDeserializeDwcDpNfsToHdfsMessage() throws Exception {
    String json =
        "{\"datasetUuid\":\"2d02d951-ff41-44d5-b018-53604e145603\",\"attempt\":42,\"pipelineSteps\":[\"nfs-to-hdfs\"],\"executionId\":0}";

    DwcDpNfsToHdfsMessage message = MAPPER.readValue(json, DwcDpNfsToHdfsMessage.class);

    assertEquals(UUID.fromString("2d02d951-ff41-44d5-b018-53604e145603"), message.getDatasetUuid());
    assertEquals(42, (int) message.getAttempt());
    assertEquals(Set.of("nfs-to-hdfs"), message.getPipelineSteps());
    assertEquals(0L, (long) message.getExecutionId());
  }
}
