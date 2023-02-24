package org.gbif.pipelines.common.process;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.MainSparkSettings;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.tasks.events.indexing.EventsIndexingConfiguration;
import org.gbif.pipelines.tasks.events.interpretation.EventsInterpretationConfiguration;
import org.gbif.pipelines.tasks.occurrences.identifier.IdentifierConfiguration;
import org.gbif.pipelines.tasks.occurrences.indexing.IndexingConfiguration;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;
import org.junit.Test;

public class ProcessRunnerBuilderTest {

  @Test(expected = NullPointerException.class)
  public void testEmptyRunner() {
    // Should
    ProcessRunnerBuilder.builder().build();
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyParameters() {

    // Should
    ProcessRunnerBuilder.builder().build();
  }

  @Test
  public void testEventIndexing() {
    // When
    String expected =
        "spark2-submit --conf spark.metrics.conf=metrics.properties "
            + "--conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines "
            + "--name=EVENTS_INTERPRETED_TO_INDEX_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s --class org.gbif.Test --master yarn "
            + "--deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --runner=SparkRunner --inputPath=tmp --targetPath=tmp "
            + "--metaFileName=event-to-index.yml --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--esHosts=http://host.com:9300 --properties=/path/ws.config --esIndexName=events --esAlias=alias "
            + "--esSchemaPath=elasticsearch/es-event-schema.json --indexNumberShards=1 --experiments=use_deprecated_read "
            + "--datasetType=SAMPLING_EVENT --dwcCore=Event --esDocumentId=internalId";

    EventsIndexingConfiguration config = new EventsIndexingConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.metricsPropertiesPath = "metrics.properties";
    config.distributedConfig.extraClassPath = "logstash-gelf.jar";
    config.distributedConfig.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.distributedConfig.deployMode = "cluster";
    config.esConfig.hosts = new String[] {"http://host.com:9300"};
    config.esConfig.schemaPath = "elasticsearch/es-event-schema.json";
    config.distributedConfig.yarnQueue = "pipelines";
    config.pipelinesConfig = "/path/ws.config";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.indexConfig.occurrenceAlias = "alias";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    PipelinesEventsInterpretedMessage message =
        new PipelinesEventsInterpretedMessage(
            datasetId,
            attempt,
            Collections.singleton(StepType.EVENTS_INTERPRETED_TO_INDEX.name()),
            100L,
            100L,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            null,
            false,
            StepRunner.STANDALONE.name());

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("EVENTS_INTERPRETED_TO_INDEX_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(
                BeamSettings.eventIndexing(config, message, IndexSettings.create("events", 1)))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testEventInterpretation() {
    // When
    String expected =
        "spark2-submit "
            + "--conf spark.metrics.conf=metrics.properties --conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines "
            + "--name=EVENTS_VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL "
            + "--runner=SparkRunner --targetPath=tmp --metaFileName=verbatim-to-event.yml --inputPath=verbatim.avro "
            + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--properties=/path/ws.config --experiments=use_deprecated_read --dwcCore=Event";

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
    Set<String> types = Collections.singleton(RecordType.ALL.name());
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
            new ValidationResult(true, true, true, null, null),
            types,
            DatasetType.SAMPLING_EVENT);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("EVENTS_VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(BeamSettings.eventInterpretation(config, message, "verbatim.avro"))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testHdfsViewSparkRunnerCommand() {
    // When
    String expected =
        "spark2-submit --name=HDFS_VIEW_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 --conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 "
            + "--conf spark.dynamicAllocation.enabled=false --conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --runner=SparkRunner "
            + "--metaFileName=occurrence-to-hdfs.yml --inputPath=tmp --targetPath=target --hdfsSiteConfig=hdfs.xml "
            + "--coreSiteConfig=core.xml --properties=/path/ws.config --numberOfShards=10 --interpretationTypes=OCCURRENCE "
            + "--experiments=use_deprecated_read";

    HdfsViewConfiguration config = new HdfsViewConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.pipelinesConfig = "/path/ws.config";
    config.repositoryTargetPath = "target";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.repositoryPath = "tmp";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    ValidationResult vr = new ValidationResult();

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            datasetId,
            attempt,
            steps,
            null,
            null,
            null,
            false,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            vr,
            Collections.singleton(OCCURRENCE.name()),
            null);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("HDFS_VIEW_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(BeamSettings.occurrenceHdfsView(config, message, 10))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testHdfsViewSparkRunnerCommandFull() {
    // When
    String expected =
        "sudo -u user spark2-submit --conf spark.metrics.conf=metrics.properties --conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines --name=HDFS_VIEW_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster "
            + "--executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d "
            + "--attempt=1 --runner=SparkRunner --metaFileName=occurrence-to-hdfs.yml --inputPath=tmp --targetPath=target --hdfsSiteConfig=hdfs.xml "
            + "--coreSiteConfig=core.xml --properties=/path/ws.config --numberOfShards=10 --interpretationTypes=OCCURRENCE "
            + "--experiments=use_deprecated_read";

    HdfsViewConfiguration config = new HdfsViewConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.metricsPropertiesPath = "metrics.properties";
    config.distributedConfig.extraClassPath = "logstash-gelf.jar";
    config.distributedConfig.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.pipelinesConfig = "/path/ws.config";
    config.repositoryTargetPath = "target";
    config.distributedConfig.yarnQueue = "pipelines";
    config.distributedConfig.otherUser = "user";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.repositoryPath = "tmp";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    ValidationResult vr = new ValidationResult();
    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            datasetId,
            attempt,
            steps,
            100L,
            null,
            null,
            false,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            vr,
            Collections.singleton(OCCURRENCE.name()),
            null);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("HDFS_VIEW_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(BeamSettings.occurrenceHdfsView(config, message, 10))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testIdentifierSparkRunnerCommand() {
    // When
    String expected =
        "spark2-submit --name=VERBATIM_TO_IDENTIFIER_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL "
            + "--runner=SparkRunner --targetPath=tmp --metaFileName=verbatim-to-identifier.yml --inputPath=verbatim.avro "
            + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--properties=/path/ws.config --experiments=use_deprecated_read --tripletValid=true "
            + "--occurrenceIdValid=true --useExtendedRecordId=true";

    IdentifierConfiguration config = new IdentifierConfiguration();
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
    config.distributedConfig.deployMode = "cluster";
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.VERBATIM_TO_IDENTIFIER.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            datasetId,
            attempt,
            types,
            steps,
            null,
            EndpointType.DWC_ARCHIVE,
            "something",
            new ValidationResult(true, true, true, null, null),
            null,
            1L,
            DatasetType.OCCURRENCE);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("VERBATIM_TO_IDENTIFIER_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(BeamSettings.occurrenceIdentifier(config, message, "verbatim.avro"))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testIdentifierSparkRunnerCommandFull() {
    // When
    String expected =
        "sudo -u user spark2-submit --conf spark.metrics.conf=metrics.properties --conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines --name=VERBATIM_TO_IDENTIFIER_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 "
            + "--conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn "
            + "--deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL --runner=SparkRunner "
            + "--targetPath=tmp --metaFileName=verbatim-to-identifier.yml --inputPath=verbatim.avro --avroCompressionType=SNAPPY "
            + "--avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml --properties=/path/ws.config "
            + "--experiments=use_deprecated_read";

    IdentifierConfiguration config = new IdentifierConfiguration();
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
    config.distributedConfig.deployMode = "cluster";
    config.distributedConfig.yarnQueue = "pipelines";
    config.distributedConfig.otherUser = "user";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.repositoryPath = "tmp";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.VERBATIM_TO_IDENTIFIER.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            datasetId,
            attempt,
            types,
            steps,
            null,
            EndpointType.DWC_ARCHIVE,
            null,
            null,
            null,
            1L,
            DatasetType.OCCURRENCE);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("VERBATIM_TO_IDENTIFIER_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(BeamSettings.occurrenceIdentifier(config, message, "verbatim.avro"))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testIndexingSparkRunnerCommand() {
    // When
    String expected =
        "spark2-submit --name=VALIDATOR_INTERPRETED_TO_INDEX_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s --class org.gbif.Test --master yarn --deploy-mode cluster "
            + "--executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --runner=SparkRunner --inputPath=tmp "
            + "--targetPath=tmp --metaFileName=occurrence-to-index.yml --hdfsSiteConfig=hdfs.xml "
            + "--coreSiteConfig=core.xml --esHosts=http://host.com:9300 --properties=/path/ws.config --esIndexName=occurrence "
            + "--experiments=use_deprecated_read --backPressure=1";

    IndexingConfiguration config = new IndexingConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.esConfig.hosts = new String[] {"http://host.com:9300"};
    config.pipelinesConfig = "/path/ws.config";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.backPressure = 1;

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    ValidationResult vr = new ValidationResult();

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            datasetId,
            attempt,
            steps,
            null,
            null,
            null,
            false,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            vr,
            Collections.singleton(OCCURRENCE.name()),
            null);

    String indexName = "occurrence";

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("VALIDATOR_INTERPRETED_TO_INDEX_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(
                BeamSettings.occurreceIndexing(
                    config, message, IndexSettings.create(indexName, null)))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testIndexingSparkRunnerCommandFull() {
    // When
    String expected =
        "spark2-submit --conf spark.metrics.conf=metrics.properties "
            + "--conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines "
            + "--name=VALIDATOR_INTERPRETED_TO_INDEX_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 --conf spark.default.parallelism=1 "
            + "--conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s --class org.gbif.Test --master yarn "
            + "--deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --runner=SparkRunner --inputPath=tmp --targetPath=tmp "
            + "--metaFileName=occurrence-to-index.yml --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--esHosts=http://host.com:9300 --properties=/path/ws.config --esIndexName=occurrence "
            + "--esSchemaPath=elasticsearch/es-validator-schema.json --experiments=use_deprecated_read";

    IndexingConfiguration config = new IndexingConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.metricsPropertiesPath = "metrics.properties";
    config.distributedConfig.extraClassPath = "logstash-gelf.jar";
    config.distributedConfig.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.esConfig.hosts = new String[] {"http://host.com:9300"};
    config.esConfig.schemaPath = "elasticsearch/es-validator-schema.json";
    config.distributedConfig.yarnQueue = "pipelines";
    config.pipelinesConfig = "/path/ws.config";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.coreSiteConfig = "core.xml";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    ValidationResult vr = new ValidationResult();
    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            datasetId,
            attempt,
            Collections.singleton(StepType.VALIDATOR_INTERPRETED_TO_INDEX.name()),
            100L,
            null,
            null,
            false,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            vr,
            steps,
            null);

    String indexName = "occurrence";

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("VALIDATOR_INTERPRETED_TO_INDEX_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(
                BeamSettings.occurreceIndexing(
                    config, message, IndexSettings.create(indexName, null)))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testInterpretationSparkRunnerCommand() {
    // When
    String expected =
        "spark2-submit --name=VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL "
            + "--runner=SparkRunner --targetPath=tmp --metaFileName=verbatim-to-occurrence.yml --inputPath=verbatim.avro "
            + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--properties=/path/ws.config --experiments=use_deprecated_read "
            + "--tripletValid=true --occurrenceIdValid=true --useExtendedRecordId=true";

    InterpreterConfiguration config = new InterpreterConfiguration();
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
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.VERBATIM_TO_INTERPRETED.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            datasetId,
            attempt,
            types,
            steps,
            null,
            EndpointType.DWC_ARCHIVE,
            "something",
            new ValidationResult(true, true, true, null, null),
            null,
            1L,
            null);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(
                BeamSettings.occurrenceInterpretation(config, message, "verbatim.avro", null))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testInterpretationValidatorCommand() {
    // When
    String expected =
        "spark2-submit --name=VALIDATOR_VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL "
            + "--runner=SparkRunner --targetPath=tmp --metaFileName=verbatim-to-occurrence.yml --inputPath=verbatim.avro "
            + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--properties=/path/ws.config --experiments=use_deprecated_read "
            + "--useMetadataWsCalls=false --tripletValid=true --occurrenceIdValid=true --useExtendedRecordId=true";

    InterpreterConfiguration config = new InterpreterConfiguration();
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
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.validatorOnly = true;

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            datasetId,
            attempt,
            types,
            steps,
            null,
            EndpointType.DWC_ARCHIVE,
            "something",
            new ValidationResult(true, true, true, null, null),
            null,
            1L,
            null);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName(
                "VALIDATOR_VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(
                BeamSettings.occurrenceInterpretation(config, message, "verbatim.avro", null))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testInterpretationSparkRunnerCommandFull() {
    // When
    String expected =
        "sudo -u user spark2-submit --conf spark.metrics.conf=metrics.properties --conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines --name=VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1 "
            + "--conf spark.default.parallelism=1 "
            + "--conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn "
            + "--deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL --runner=SparkRunner "
            + "--targetPath=tmp --metaFileName=verbatim-to-occurrence.yml --inputPath=verbatim.avro --avroCompressionType=SNAPPY "
            + "--avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml --properties=/path/ws.config "
            + "--experiments=use_deprecated_read";

    InterpreterConfiguration config = new InterpreterConfiguration();
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
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.distributedConfig.yarnQueue = "pipelines";
    config.distributedConfig.otherUser = "user";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.repositoryPath = "tmp";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.VERBATIM_TO_INTERPRETED.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            datasetId,
            attempt,
            types,
            steps,
            null,
            EndpointType.DWC_ARCHIVE,
            null,
            null,
            null,
            1L,
            null);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .sparkSettings(TestSparkSettings.create(1, "1G", 1))
            .sparkAppName("VERBATIM_TO_INTERPRETED_de7ffb5e-c07b-42dc-8a88-f67a4465fe3d_1")
            .beamConfigFn(
                BeamSettings.occurrenceInterpretation(config, message, "verbatim.avro", null))
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @AllArgsConstructor(staticName = "create")
  private static class TestSparkSettings implements MainSparkSettings {

    private final int parallelism;
    private final String executorMemory;
    private final int executorNumbers;

    @Override
    public int getParallelism() {
      return parallelism;
    }

    @Override
    public String getExecutorMemory() {
      return executorMemory;
    }

    @Override
    public int getExecutorNumbers() {
      return executorNumbers;
    }
  }
}
