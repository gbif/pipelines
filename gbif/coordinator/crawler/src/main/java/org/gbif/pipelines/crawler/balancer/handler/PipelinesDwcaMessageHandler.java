package org.gbif.pipelines.crawler.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;

/**
 * Populates and sends the {@link PipelinesDwcaMessage} message, the main method is {@link
 * PipelinesDwcaMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesDwcaMessageHandler {

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesDwcaMessage - {}", message);

    ObjectMapper mapper = new ObjectMapper();
    PipelinesDwcaMessage m = mapper.readValue(message.getPayload(), PipelinesDwcaMessage.class);

    OccurrenceValidationReport occurrenceReport = m.getValidationReport().getOccurrenceReport();

    PipelinesDwcaMessage outputMessage =
        new PipelinesDwcaMessage(
            m.getDatasetUuid(),
            m.getDatasetType(),
            m.getSource(),
            m.getAttempt(),
            new DwcaValidationReport(
                m.getDatasetUuid(),
                new OccurrenceValidationReport(
                    occurrenceReport.getCheckedRecords(),
                    occurrenceReport.getUniqueTriplets(),
                    occurrenceReport.getRecordsWithInvalidTriplets(),
                    occurrenceReport.getUniqueOccurrenceIds(),
                    occurrenceReport.getRecordsMissingOccurrenceId(),
                    occurrenceReport.isAllRecordsChecked())),
            m.getPipelineSteps(),
            m.getEndpointType(),
            m.getPlatform(),
            m.getExecutionId(),
            m.isValidator());

    publisher.send(outputMessage);

    log.info("The message has been sent - {}", outputMessage);
  }
}
