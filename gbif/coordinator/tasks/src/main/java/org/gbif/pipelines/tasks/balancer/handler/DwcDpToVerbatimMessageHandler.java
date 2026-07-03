package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
    String metricsPath =
        String.join(
            "/",
            config.stepConfig.repositoryPath,
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

    publisher.send(out);

    log.info(
        "Outgoing dataset: {}, executionID: {}, routingKey: {}, attempt: {}, max observed records: {}",
        out.getDatasetUuid(),
        out.getExecutionId(),
        out.getRoutingKey(),
        out.getAttempt(),
        maxCount);
  }
}
