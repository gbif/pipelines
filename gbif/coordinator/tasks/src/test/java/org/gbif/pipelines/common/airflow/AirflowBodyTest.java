package org.gbif.pipelines.common.airflow;

import static junit.framework.TestCase.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.gbif.api.model.pipelines.InterpretationType;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.configs.AirflowConfiguration;
import org.gbif.pipelines.common.process.AirflowRunnerBuilder;
import org.gbif.pipelines.common.process.BeamSettings;
import org.gbif.pipelines.tasks.events.interpretation.EventsInterpretationConfiguration;
import org.junit.Test;

public class AirflowBodyTest {
  @Test
  public void testAirflowBodyCreation() {
    AirflowConfiguration conf = createConf();

    AirflowBody testBody = new AirflowBody(conf);
    List<String> args = new ArrayList<String>();
    args.add("--datasetId=647490ab-72e9-4dd6-ac83-8f771494df36");
    args.add("--attempt=273");
    testBody.setArgs(args);
    ObjectMapper mapper = new ObjectMapper();
    String result = "";
    try {
      result = mapper.writeValueAsString(testBody);
    } catch (JsonProcessingException e) {
      System.out.println("Failed parsing json");
    }

    String expected =
        "{\"main\":\"\",\"args\":[\"--datasetId=647490ab-72e9-4dd6-ac83-8f771494df36\",\"--attempt=273\"],\"driverCores\":\"1000m\",\"driverMemory\":\"1Gi\",\"executorInstances\":2,\"executorCores\":\"1000m\",\"executorMemory\":\"1Gi\",\"clusterName\":\"test-cluster\"}";

    assertEquals(expected, result);
  }

  @Test
  public void testAirflowBodyCreationBasedOnMessage() {
    AirflowConfiguration conf = createConf();

    EventsInterpretationConfiguration config = new EventsInterpretationConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.avroConfig.compressionType = "SNAPPY";
    config.avroConfig.syncInterval = 1;
    config.pipelinesConfig = "/path/ws.config";
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.metricsPropertiesPath = "metrics.properties";
    config.distributedConfig.extraClassPath = "logstash-gelf.jar";
    config.distributedConfig.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.distributedConfig.yarnQueue = "pipelines";
    config.distributedConfig.deployMode = "cluster";
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(InterpretationType.RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.EVENTS_VERBATIM_TO_INTERPRETED.name());
    PipelinesEventsMessage message =
        new PipelinesEventsMessage(
            datasetId,
            attempt,
            steps,
            100L,
            100L,
            StepRunner.DISTRIBUTED.name(),
            false,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            new PipelinesVerbatimMessage.ValidationResult(true, true, true, null, null),
            types,
            DatasetType.SAMPLING_EVENT);

    AirflowRunner airRunner =
        AirflowRunnerBuilder.builder()
            .airflowConfiguration(conf)
            .dagId(AirflowConfiguration.SPARK_DAG_NAME)
            .main("org.gbif.ATestClass")
            .dagRunId(
                "EVENTS_VERBATIM_TO_INTERPRETED"
                    + "_"
                    + message.getDatasetUuid()
                    + "_"
                    + message.getAttempt())
            .beamConfigFn(BeamSettings.eventInterpretation(config, message, "verbatim.avro"))
            .build()
            .buildAirflowRunner();

    AirflowBody body = airRunner.getBody();

    ObjectMapper mapper = new ObjectMapper();
    String result = "";
    try {
      result = mapper.writeValueAsString(body);
    } catch (JsonProcessingException e) {
      System.out.println(e.getMessage());
      System.out.println("Failed parsing json");
    }

    String expected =
        "{\"main\":\"org.gbif.ATestClass\",\"args\":[\"--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d\",\"--attempt=1\",\"--interpretationTypes=ALL\",\"--runner=SparkRunner\",\"--targetPath=tmp\",\"--metaFileName=verbatim-to-event.yml\",\"--inputPath=verbatim.avro\",\"--avroCompressionType=SNAPPY\",\"--avroSyncInterval=1\",\"--hdfsSiteConfig=hdfs.xml\",\"--coreSiteConfig=core.xml\",\"--properties=/path/ws.config\",\"--experiments=use_deprecated_read\",\"--dwcCore=Event\"],\"driverCores\":\"1000m\",\"driverMemory\":\"1Gi\",\"executorInstances\":2,\"executorCores\":\"1000m\",\"executorMemory\":\"1Gi\",\"clusterName\":\"test-cluster\"}";

    assertEquals(expected, result);
  }

  private AirflowConfiguration createConf() {
    AirflowConfiguration conf = new AirflowConfiguration();
    conf.useAirflow = true;
    conf.airflowUser = "aUser";
    conf.airflowPass = "aPassword";
    conf.maxCoresDriver = "1000m";
    conf.maxMemoryDriver = "1Gi";
    conf.maxCoresWorkers = "1000m";
    conf.maxMemoryWorkers = "1Gi";
    conf.numberOfWorkers = 2;
    conf.airflowCluster = "test-cluster";

    return conf;
  }
}
