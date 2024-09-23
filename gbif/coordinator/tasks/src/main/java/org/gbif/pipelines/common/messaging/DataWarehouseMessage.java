package org.gbif.pipelines.common.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Set;
import java.util.UUID;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewMessage;

public class DataWarehouseMessage extends PipelinesHdfsViewMessage {

  public DataWarehouseMessage() {}

  @JsonCreator
  public DataWarehouseMessage(
      @JsonProperty("datasetUuid") UUID datasetUuid,
      @JsonProperty("attempt") int attempt,
      @JsonProperty("pipelineSteps") Set<String> pipelineSteps,
      @JsonProperty("runner") String runner,
      @JsonProperty("executionId") Long executionId) {
    super(datasetUuid, attempt, pipelineSteps, runner, executionId);
  }
}
