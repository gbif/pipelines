package org.gbif.pipelines.tasks.balancer.handler;

import static org.gbif.api.model.pipelines.StepType.NFS_TO_HDFS;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.ExchangeType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpMetadataSyncFinishedMessage;
import org.gbif.common.messaging.api.messages.DwcDpNfsToHdfsMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.dp.descriptor.DataPackageDescriptor;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.pipelines.tasks.balancer.BalancerConfiguration;

@Slf4j
public class DwcDpNfsToHdfsMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  public static final String DISTRIBUTED = ".distributed";
  public static final String STANDALONE = ".standalone";

  public static void handle(
      BalancerConfiguration config, MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    DwcDpMetadataSyncFinishedMessage m =
        MAPPER.readValue(message.getPayload(), DwcDpMetadataSyncFinishedMessage.class);

    String datasetKey = m.getDatasetUuid().toString();
    log.info("Reading from {}/{}", config.dwcdpRepositoryPath, datasetKey);
    Path archivePath = Paths.get(config.dwcdpRepositoryPath, datasetKey);

    DataPackageDescriptor descriptor =
        new JacksonDataPackageParser(MAPPER)
            .parse(Paths.get(config.dwcdpRepositoryPath, datasetKey, "datapackage.json"));
    boolean containsOccurrences =
        descriptor.resources().stream().anyMatch(r -> r.name().equalsIgnoreCase("occurrence"));
    boolean containsEvents =
        descriptor.resources().stream().anyMatch(r -> r.name().equalsIgnoreCase("event"));

    long fileSizeBytes = getFileSizeBytes(archivePath);
    long switchFileSizeBytes = config.switchFileSizeMb * 1024L * 1024L;

    Set<String> pipelineSteps =
        PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents)
            .getAllNodesFor(Set.of(NFS_TO_HDFS))
            .stream()
            .map(StepType::name)
            .collect(Collectors.toSet());

    DwcDpNfsToHdfsMessage out =
        new DwcDpNfsToHdfsMessage(
            m.getDatasetUuid(),
            m.getAttempt(),
            pipelineSteps,
            null,
            containsOccurrences,
            containsEvents);
    if (fileSizeBytes > switchFileSizeBytes) {
      publisher.send(out, ExchangeType.OCCURRENCE.getValue(), out.getRoutingKey() + DISTRIBUTED);
      log.info("Routing to DISTRIBUTED, dataset {}, size {} bytes", datasetKey, fileSizeBytes);
    } else {
      publisher.send(out, ExchangeType.OCCURRENCE.getValue(), out.getRoutingKey() + STANDALONE);
      log.info("Routing to STANDALONE, dataset {}, size {} bytes", datasetKey, fileSizeBytes);
    }
  }

  static long getFileSizeBytes(Path archivePath) throws IOException {
    try (Stream<Path> stream = Files.walk(archivePath)) {
      return stream.filter(Files::isRegularFile).mapToLong(p -> p.toFile().length()).sum();
    }
  }
}
