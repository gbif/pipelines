package org.gbif.pipelines.tasks.balancer;

import static org.gbif.api.model.pipelines.StepRunner.DISTRIBUTED;
import static org.gbif.api.model.pipelines.StepType.VERBATIM_TO_IDENTIFIER;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BalancerCallbackIT {

  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void verbatimMessageHandlerStandaloneTest() {
    // State
    BalancerConfiguration config = createConfig();
    config.switchRecordsNumber = 100;

    BalancerCallback callback = new BalancerCallback(config, PUBLISHER);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    // Resource file verbatim-to-identifier.yml contains 10 records
    long records = 1L;

    PipelinesVerbatimMessage mainMessage = createPipelinesVerbatimMessage(uuid, attempt, records);
    PipelinesBalancerMessage wrappedMessage = createPipelinesBalancerMessage(mainMessage);

    // When
    callback.handleMessage(wrappedMessage);

    // Should
    List<Message> messages = PUBLISHER.getMessages();

    Assert.assertEquals(1, messages.size());

    PipelinesVerbatimMessage message = (PipelinesVerbatimMessage) messages.get(0);
    Assert.assertEquals(StepRunner.STANDALONE.name(), message.getRunner());
  }

  @Test
  public void verbatimMessageHandlerDistributedTest() {
    // State
    BalancerConfiguration config = createConfig2();
    config.switchRecordsNumber = 1;

    BalancerCallback callback = new BalancerCallback(config, PUBLISHER);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    // Resource file verbatim-to-identifier.yml contains 10 records
    long records = 1L;

    PipelinesVerbatimMessage mainMessage = createPipelinesVerbatimMessage(uuid, attempt, records);
    PipelinesBalancerMessage wrappedMessage = createPipelinesBalancerMessage(mainMessage);

    // When
    callback.handleMessage(wrappedMessage);

    // Should
    List<Message> messages = PUBLISHER.getMessages();

    Assert.assertEquals(1, messages.size());

    PipelinesVerbatimMessage message = (PipelinesVerbatimMessage) messages.get(0);
    Assert.assertEquals(StepRunner.DISTRIBUTED.name(), message.getRunner());
  }

  @Test
  public void verbatimMessageHandlerNullRecordsTest() {
    // State
    BalancerConfiguration config = createConfig();
    config.switchRecordsNumber = 100;

    BalancerCallback callback = new BalancerCallback(config, PUBLISHER);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    // Resource file verbatim-to-identifier.yml contains 10 records
    Long records = null;

    PipelinesVerbatimMessage mainMessage = createPipelinesVerbatimMessage(uuid, attempt, records);
    PipelinesBalancerMessage wrappedMessage = createPipelinesBalancerMessage(mainMessage);

    // When
    callback.handleMessage(wrappedMessage);

    // Should
    List<Message> messages = PUBLISHER.getMessages();

    Assert.assertEquals(1, messages.size());

    PipelinesVerbatimMessage message = (PipelinesVerbatimMessage) messages.get(0);
    Assert.assertEquals(StepRunner.STANDALONE.name(), message.getRunner());
  }

  private PipelinesBalancerMessage createPipelinesBalancerMessage(
      PipelineBasedMessage outgoingMessage) {
    String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
    String messagePayload = outgoingMessage.toString();
    return new PipelinesBalancerMessage(nextMessageClassName, messagePayload);
  }

  private PipelinesVerbatimMessage createPipelinesVerbatimMessage(
      UUID uuid, int attempt, Long records) {

    ValidationResult validationResult = new ValidationResult(true, true, false, records, null);

    return new PipelinesVerbatimMessage(
        uuid,
        attempt,
        Collections.singleton(RecordType.ALL.name()),
        Collections.singleton(VERBATIM_TO_IDENTIFIER.name()),
        DISTRIBUTED.name(),
        EndpointType.DWC_ARCHIVE,
        null,
        validationResult,
        null,
        null,
        DatasetType.SAMPLING_EVENT);
  }

  private BalancerConfiguration createConfig() {
    BalancerConfiguration config = new BalancerConfiguration();

    // Main
    config.switchFilesNumber = 1;
    config.switchFileSizeMb = 1;
    config.switchRecordsNumber = 1;
    config.validatorSwitchRecordsNumber = 1;

    // Step config
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();

    return config;
  }

  private BalancerConfiguration createConfig2() {
    BalancerConfiguration config = new BalancerConfiguration();

    // Main
    config.switchFilesNumber = 1;
    config.switchFileSizeMb = 0;
    config.switchRecordsNumber = 1;
    config.validatorSwitchRecordsNumber = 1;

    // Step config
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();

    return config;
  }
}
