package org.gbif.pipelines.tasks.balancer.handler;

import static org.gbif.api.model.pipelines.StepType.DWCDP_STAGE;
import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

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
    publishStage(publisher, out);
    log.info(
        "Routing to STANDALONE, dataset {}, attempt: {}",
        m.getDatasetUuid().toString(),
        m.getAttempt());
  }

  /**
   * Routes orchestrator-created stage messages without replacing their validator workflow steps.
   */
  public static void handleStage(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {
    DwcDpStageMessage stageMessage =
        MAPPER.readValue(message.getPayload(), DwcDpStageMessage.class);

    publishStage(publisher, stageMessage);
  }

  private static void publishStage(MessagePublisher publisher, DwcDpStageMessage message)
      throws IOException {
    boolean validator = isValidator(message.getPipelineSteps());
    String routingKey = (validator ? "validator." : "") + message.getRoutingKey() + STANDALONE;

    publisher.send(message, ExchangeType.OCCURRENCE.getValue(), routingKey);
    log.info(
        "Routing DWCDP stage dataset {}, attempt {} to {}",
        message.getDatasetUuid(),
        message.getAttempt(),
        routingKey);
  }

  static long getFileSizeBytes(Path archivePath) throws IOException {
    try (Stream<Path> stream = Files.walk(archivePath)) {
      return stream.filter(Files::isRegularFile).mapToLong(p -> p.toFile().length()).sum();
    }
  }
}
