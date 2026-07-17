package org.gbif.pipelines.tasks.balancer.handler;

import static org.gbif.api.model.pipelines.StepType.DWCDP_STAGE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.ExchangeType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpMetadataSyncFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.pipelines.tasks.balancer.BalancerConfiguration;

@Slf4j
public class DwcDpStageMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  public static final String DISTRIBUTED = ".distributed";
  public static final String STANDALONE = ".standalone";

  public static void handle(
      BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    DwcDpMetadataSyncFinishedMessage m =
        MAPPER.readValue(message.getPayload(), DwcDpMetadataSyncFinishedMessage.class);

    boolean containsOccurrences = m.isContainsOccurrences();
    boolean containsEvents = m.isContainsEvents();

    Set<String> pipelineSteps =
        PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents)
            .getAllNodesFor(Set.of(DWCDP_STAGE))
            .stream()
            .map(StepType::name)
            .collect(Collectors.toSet());

    DwcDpStageMessage out =
        new DwcDpStageMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            pipelineSteps,
            null,
            containsOccurrences,
            containsEvents);
    publisher.send(out, ExchangeType.OCCURRENCE.getValue(), out.getRoutingKey() + STANDALONE);
    log.info(
        "Routing to STANDALONE, dataset {}, attempt: {}",
        m.getDatasetUuid().toString(),
        m.getAttempt());
  }

  static long getFileSizeBytes(Path archivePath) throws IOException {
    try (Stream<Path> stream = Files.walk(archivePath)) {
      return stream.filter(Files::isRegularFile).mapToLong(p -> p.toFile().length()).sum();
    }
  }
}
