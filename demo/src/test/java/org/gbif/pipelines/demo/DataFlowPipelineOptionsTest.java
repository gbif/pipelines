package org.gbif.pipelines.demo;

import org.gbif.pipelines.core.config.DataFlowPipelineOptions;
import org.gbif.pipelines.core.config.RecordInterpretation;
import org.gbif.pipelines.core.config.TargetPath;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

public class DataFlowPipelineOptionsTest {

  @Test
  public void testWithDefaultTargetDirectoryAndSettingTargetPathsProgrammatically() {
    DataFlowPipelineOptions options =
      PipelineOptionsFactory.fromArgs(new String[] {"--datasetId=" + UUID.randomUUID().toString(),
        "--inputFile=abc123"}).withValidation().as(DataFlowPipelineOptions.class);
    String defaultTargetDirectory = "/Users/clf358/gbif/data";
    options.setDefaultTargetDirectory(defaultTargetDirectory);

    Map<RecordInterpretation, String> interpretationTargetMap = new HashMap<>();
    interpretationTargetMap.put(RecordInterpretation.RAW_OCCURRENCE,
                                new TargetPath(defaultTargetDirectory, "raw").filePath());
    interpretationTargetMap.put(RecordInterpretation.INTERPRETED_OCURENCE,
                                new TargetPath(defaultTargetDirectory, "interpreted").filePath());
    interpretationTargetMap.put(RecordInterpretation.TEMPORAL,
                                new TargetPath(defaultTargetDirectory, "temporal").filePath());
    interpretationTargetMap.put(RecordInterpretation.TEMPORAL_ISSUE,
                                new TargetPath(defaultTargetDirectory, "temporal_issue").filePath());
    interpretationTargetMap.put(RecordInterpretation.LOCATION,
                                new TargetPath(defaultTargetDirectory, "location").filePath());
    interpretationTargetMap.put(RecordInterpretation.LOCATION_ISSUE,
                                new TargetPath(defaultTargetDirectory, "location_issue").filePath());
    interpretationTargetMap.put(RecordInterpretation.INTERPRETED_ISSUE,
                                new TargetPath(defaultTargetDirectory, "interpreted").filePath());
    interpretationTargetMap.put(RecordInterpretation.TEMP_DwCA_PATH,
                                new TargetPath(defaultTargetDirectory, "temp_file").filePath());

    options.setTargetPaths(interpretationTargetMap);
    options.setDatasetId(UUID.randomUUID().toString());

    System.out.println(options.getDatasetId());
    System.out.println(options.getInputFile());
    System.out.println(options.getDefaultTargetDirectory());
    System.out.println(options.getTargetPaths());

  }

  @Test
  public void testImplicitTargetPaths() throws IOException {
    DataFlowPipelineOptions options =
      PipelineOptionsFactory.fromArgs(new String[] {"--datasetId=" + UUID.randomUUID().toString(),
        "--inputFile=abc123"}).withValidation().as(DataFlowPipelineOptions.class);

    System.out.println(options.getDatasetId());
    System.out.println(options.getInputFile());
    System.out.println(options.getDefaultTargetDirectory());
    System.out.println(options.getTargetPaths());

    ObjectMapper mapper = new ObjectMapper();
    System.out.println(mapper.writeValueAsString(options.getTargetPaths()));
    Map<RecordInterpretation, String> map = options.getTargetPaths();
    for (Map.Entry<RecordInterpretation, String> entry : map.entrySet()) {
      System.out.println(entry.getKey() + "->" + entry.getValue());
    }
  }

  @Test
  public void testExplicitTargetPaths() {
    String targetPathMap =
      "{\"TEMPORAL_ISSUE\":\"/Users/clf358/gbif-data/some-issue\",\"TEMP_DwCA_PATH\":\"/Users/clf358/gbif-data/temp\",\"LOCATION\":\"/Users/clf358/gbif-data/location\",\"RAW_OCCURRENCE\":\"/Users/clf358/gbif-data/raw_data\",\"TEMPORAL\":\"/Users/clf358/gbif-data/temporal\",\"LOCATION_ISSUE\":\"/Users/clf358/gbif-data/location_issue\",\"GBIF_BACKBONE\":\"/Users/clf358/gbif-data/gbif-backbone\",\"INTERPRETED_ISSUE\":\"/Users/clf358/gbif-data/interpreted-issue\",\"INTERPRETED_OCURENCE\":\"/Users/clf358/gbif-data/interpreted\",\"VERBATIM\":\"/Users/clf358/gbif-data/verbatim\"}";
    DataFlowPipelineOptions options =
      PipelineOptionsFactory.fromArgs(new String[] {"--datasetId=" + UUID.randomUUID().toString(), "--inputFile=abc123",
        "--targetPaths=" + targetPathMap}).withValidation().as(DataFlowPipelineOptions.class);

    System.out.println(options.getDatasetId());
    System.out.println(options.getInputFile());
    System.out.println(options.getDefaultTargetDirectory());
    System.out.println(options.getTargetPaths());
    System.out.println("HDFS Configuration Directory:" + options.getHDFSConfigurationDirectory());
    Map<RecordInterpretation, String> map = options.getTargetPaths();
    for (Map.Entry<RecordInterpretation, String> entry : map.entrySet()) {
      System.out.println(entry.getKey() + "->" + entry.getValue());
    }
  }

}
