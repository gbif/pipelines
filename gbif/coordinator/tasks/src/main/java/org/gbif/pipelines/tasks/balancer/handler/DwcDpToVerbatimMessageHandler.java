package org.gbif.pipelines.tasks.balancer.handler;

import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.ExchangeType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
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
    boolean validator = isValidator(m.getPipelineSteps());
    String repositoryPath =
        validator ? config.validatorRepositoryPath : config.stepConfig.repositoryPath;

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    String metricsPath =
        String.join(
            "/",
            repositoryPath,
            datasetId,
            String.valueOf(m.getAttempt()),
            PipelinesVariables.Pipeline.DWCDP_STAGE + ".yml");
    long maxCount = HdfsUtils.getLongByKey(hdfsConfigs, metricsPath, "COUNT_MAX").orElse(0L);

    DwcDpToVerbatimMessage out =
        new DwcDpToVerbatimMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            m.getPipelineSteps(),
            m.getExecutionId(),
            m.isContainsOccurrences(),
            m.isContainsEvents(),
            maxCount <= config.switchFileSizeMb);

    if (validator) {
      publisher.send(out, ExchangeType.OCCURRENCE.getValue(), "validator." + out.getRoutingKey());
    } else {
      publisher.send(out);
    }

    log.info(
        "Outgoing dataset: {}, executionID: {}, routingKey: {}, attempt: {}, max observed records: {}",
        out.getDatasetUuid(),
        out.getExecutionId(),
        validator ? "validator." + out.getRoutingKey() : out.getRoutingKey(),
        out.getAttempt(),
        maxCount);
  }
}
