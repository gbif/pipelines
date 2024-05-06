package org.gbif.pipelines.common.process;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.OCCURRENCE;
import static org.gbif.api.model.pipelines.StepRunner.DISTRIBUTED;
import static org.gbif.api.model.pipelines.StepType.VERBATIM_TO_INTERPRETED;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.indexing.IndexSettings;
import org.gbif.pipelines.common.process.BeamParametersBuilder.BeamParameters;
import org.gbif.pipelines.tasks.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.occurrences.identifier.IdentifierConfiguration;
import org.gbif.pipelines.tasks.occurrences.indexing.IndexingConfiguration;
import org.gbif.pipelines.tasks.occurrences.interpretation.InterpreterConfiguration;
import org.junit.Test;

public class BeamParametersBuilderTest {

  @Test
  public void occurrenceInterpretationTest() {

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.stepConfig.repositoryPath = "repositoryPath";
    config.processRunner = DISTRIBUTED.name();
    config.pipelinesConfig = "pipelines.yaml";
    config.stepConfig.coreSiteConfig = "coreSiteConfig";
    config.stepConfig.hdfsSiteConfig = "hdfsSiteConfig";

    UUID uuid = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
    int attempt = 60;
    ValidationResult validationResult = new ValidationResult(true, true, false, 100L, null);

    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            uuid,
            attempt,
            Collections.singleton(RecordType.ALL.name()),
            Collections.singleton(VERBATIM_TO_INTERPRETED.name()),
            DISTRIBUTED.name(),
            EndpointType.DWC_ARCHIVE,
            null,
            validationResult,
            null,
            null,
            null);

    String inputPath = "inputPath";
    String dataFormat = "dataFormat";
    int numberOfShards = 1;

    BeamParameters resultParameters =
        BeamParametersBuilder.occurrenceInterpretation(
            config, message, inputPath, dataFormat, numberOfShards);

    List<String> resultList =
        resultParameters.toList().stream().sorted().collect(Collectors.toList());

    List<String> expectedList =
        Arrays.asList(
            "--attempt=60",
            "--avroCompressionType=snappy",
            "--avroSyncInterval=2097152",
            "--coreSiteConfig=coreSiteConfig",
            "--datasetId=9bed66b3-4caa-42bb-9c93-71d7ba109dad",
            "--defaultDateFormat=dataFormat",
            "--experiments=use_deprecated_read",
            "--hdfsSiteConfig=hdfsSiteConfig",
            "--inputPath=inputPath",
            "--interpretationTypes=ALL",
            "--metaFileName=verbatim-to-occurrence.yml",
            "--numberOfShards=1",
            "--occurrenceIdValid=true",
            "--properties=pipelines.yaml",
            "--runner=SparkRunner",
            "--targetPath=repositoryPath",
            "--tripletValid=true",
            "--useExtendedRecordId=false");

    assertEquals(expectedList, resultList);
  }

  @Test
  public void occurrenceIdentifierTest() {

    IdentifierConfiguration config = new IdentifierConfiguration();
    config.stepConfig.repositoryPath = "repositoryPath";
    config.pipelinesConfig = "pipelines.yaml";
    config.stepConfig.coreSiteConfig = "coreSiteConfig";
    config.stepConfig.hdfsSiteConfig = "hdfsSiteConfig";

    UUID uuid = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
    int attempt = 60;
    ValidationResult validationResult = new ValidationResult(true, true, false, 100L, null);

    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            uuid,
            attempt,
            Collections.singleton(RecordType.ALL.name()),
            Collections.singleton(VERBATIM_TO_INTERPRETED.name()),
            DISTRIBUTED.name(),
            EndpointType.DWC_ARCHIVE,
            null,
            validationResult,
            null,
            null,
            null);

    String inputPath = "inputPath";
    int numberOfShards = 1;

    BeamParameters resultParameters =
        BeamParametersBuilder.occurrenceIdentifier(config, message, inputPath, numberOfShards);

    List<String> resultList =
        resultParameters.toList().stream().sorted().collect(Collectors.toList());

    List<String> expectedList =
        Arrays.asList(
            "--attempt=60",
            "--avroCompressionType=snappy",
            "--avroSyncInterval=2097152",
            "--coreSiteConfig=coreSiteConfig",
            "--datasetId=9bed66b3-4caa-42bb-9c93-71d7ba109dad",
            "--experiments=use_deprecated_read",
            "--hdfsSiteConfig=hdfsSiteConfig",
            "--inputPath=inputPath",
            "--interpretationTypes=ALL",
            "--metaFileName=verbatim-to-identifier.yml",
            "--numberOfShards=1",
            "--occurrenceIdValid=true",
            "--properties=pipelines.yaml",
            "--runner=SparkRunner",
            "--targetPath=repositoryPath",
            "--tripletValid=true",
            "--useExtendedRecordId=false");

    assertEquals(expectedList, resultList);
  }

  @Test
  public void occurrenceIndexingTest() {

    IndexingConfiguration config = new IndexingConfiguration();
    config.stepConfig.repositoryPath = "repositoryPath";
    config.pipelinesConfig = "pipelines.yaml";
    config.stepConfig.coreSiteConfig = "coreSiteConfig";
    config.stepConfig.hdfsSiteConfig = "hdfsSiteConfig";
    config.esConfig.hosts = new String[] {"host"};

    UUID uuid = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
    int attempt = 60;
    ValidationResult validationResult = new ValidationResult(true, true, false, 100L, null);

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            uuid,
            attempt,
            new HashSet<>(Arrays.asList(StepType.HDFS_VIEW.name(), StepType.FRAGMENTER.name())),
            (long) 10,
            null,
            StepRunner.STANDALONE.name(),
            true,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            validationResult,
            Collections.singleton(OCCURRENCE.name()),
            null);

    IndexSettings indexSettings = IndexSettings.create("index", 5);

    BeamParameters resultParameters =
        BeamParametersBuilder.occurrenceIndexing(config, message, indexSettings);

    List<String> resultList =
        resultParameters.toList().stream().sorted().collect(Collectors.toList());

    List<String> expectedList =
        Arrays.asList(
            "--attempt=60",
            "--coreSiteConfig=coreSiteConfig",
            "--datasetId=9bed66b3-4caa-42bb-9c93-71d7ba109dad",
            "--esHosts=host",
            "--esIndexName=index",
            "--experiments=use_deprecated_read",
            "--hdfsSiteConfig=hdfsSiteConfig",
            "--indexNumberShards=5",
            "--inputPath=repositoryPath",
            "--metaFileName=occurrence-to-index.yml",
            "--properties=pipelines.yaml",
            "--runner=SparkRunner",
            "--targetPath=repositoryPath");

    assertEquals(expectedList, resultList);
  }

  @Test
  public void occurrenceHdfsViewTest() {

    HdfsViewConfiguration config = new HdfsViewConfiguration();
    config.stepConfig.repositoryPath = "repositoryPath";
    config.pipelinesConfig = "pipelines.yaml";
    config.stepConfig.coreSiteConfig = "coreSiteConfig";
    config.stepConfig.hdfsSiteConfig = "hdfsSiteConfig";
    config.repositoryTargetPath = "repositoryTargetPath";

    UUID uuid = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
    int attempt = 60;
    ValidationResult validationResult = new ValidationResult(true, true, false, 100L, null);

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            uuid,
            attempt,
            new HashSet<>(Arrays.asList(StepType.HDFS_VIEW.name(), StepType.FRAGMENTER.name())),
            (long) 10,
            null,
            StepRunner.STANDALONE.name(),
            true,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            validationResult,
            Collections.singleton(OCCURRENCE.name()),
            null);

    BeamParameters resultParameters = BeamParametersBuilder.occurrenceHdfsView(config, message, 5);

    List<String> resultList =
        resultParameters.toList().stream().sorted().collect(Collectors.toList());

    List<String> expectedList =
        Arrays.asList(
            "--attempt=60",
            "--coreSiteConfig=coreSiteConfig",
            "--datasetId=9bed66b3-4caa-42bb-9c93-71d7ba109dad",
            "--experiments=use_deprecated_read",
            "--hdfsSiteConfig=hdfsSiteConfig",
            "--inputPath=repositoryPath",
            "--interpretationTypes=OCCURRENCE",
            "--metaFileName=occurrence-to-hdfs.yml",
            "--numberOfShards=5",
            "--properties=pipelines.yaml",
            "--runner=SparkRunner",
            "--targetPath=repositoryTargetPath");

    assertEquals(expectedList, resultList);
  }
}
