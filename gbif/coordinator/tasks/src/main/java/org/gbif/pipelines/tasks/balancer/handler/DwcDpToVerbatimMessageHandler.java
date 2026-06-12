package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Paths;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.pipelines.tasks.balancer.BalancerConfiguration;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcDpToVerbatimMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void handle(
      BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process DwcDpToVerbatimMessage - {}", message);

    DwcDpToVerbatimMessage m = MAPPER.readValue(message.getPayload(), DwcDpToVerbatimMessage.class);

    String datasetId = m.getDatasetUuid().toString();
    // TODO, currently just use original file size from nfs, change to inspect rowCount or otherwise
    // size in hdfs
    long fileSizeBytes =
        DwcDpNfsToHdfsMessageHandler.getFileSizeBytes(
            Paths.get(config.dwcdpRepositoryPath, datasetId));
    long switchFileSizeBytes = config.switchFileSizeMb * 1024L * 1024L;

    DwcDpToVerbatimMessage out =
        new DwcDpToVerbatimMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getPipelineSteps(),
            m.getExecutionId(),
            m.isContainsOccurrences(),
            m.isContainsEvents(),
            fileSizeBytes <= switchFileSizeBytes);

    publisher.send(out);

    log.info(
        "Outgoing dataset: {}, executionID: {}, routingKey: {}, attempt: {}, size: {} bytes",
        out.getDatasetUuid(),
        out.getExecutionId(),
        out.getRoutingKey(),
        out.getAttempt(),
        fileSizeBytes);
  }
}
