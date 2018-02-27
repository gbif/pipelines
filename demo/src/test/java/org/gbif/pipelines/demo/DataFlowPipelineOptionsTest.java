package org.gbif.pipelines.demo;

import org.gbif.pipelines.core.config.DataFlowPipelineOptions;
import org.gbif.pipelines.core.config.Interpretation;
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

    Map<Interpretation, String> interpretationTargetMap = new HashMap<>();
    interpretationTargetMap.put(Interpretation.RAW_OCCURRENCE,
                                new TargetPath(defaultTargetDirectory, "raw").getFullPath());
    interpretationTargetMap.put(Interpretation.INTERPRETED_OCURENCE,
                                new TargetPath(defaultTargetDirectory, "interpreted").getFullPath());
    interpretationTargetMap.put(Interpretation.TEMPORAL,
                                new TargetPath(defaultTargetDirectory, "temporal").getFullPath());
    interpretationTargetMap.put(Interpretation.TEMPORAL_ISSUE,
                                new TargetPath(defaultTargetDirectory, "temporal_issue").getFullPath());
    interpretationTargetMap.put(Interpretation.LOCATION,
                                new TargetPath(defaultTargetDirectory, "location").getFullPath());
    interpretationTargetMap.put(Interpretation.LOCATION_ISSUE,
                                new TargetPath(defaultTargetDirectory, "location_issue").getFullPath());
    interpretationTargetMap.put(Interpretation.INTERPRETED_ISSUE,
                                new TargetPath(defaultTargetDirectory, "interpreted").getFullPath());
    interpretationTargetMap.put(Interpretation.TEMP_DWCA_PATH,
                                new TargetPath(defaultTargetDirectory, "temp_file").getFullPath());

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
    Map<Interpretation, String> map = options.getTargetPaths();
    for (Map.Entry<Interpretation, String> entry : map.entrySet()) {
      System.out.println(entry.getKey() + "->" + entry.getValue());
    }
  }

  @Test
  public void testExplicitTargetPaths() {
    String targetPathMap =
      "{\"TEMPORAL_ISSUE\":\"/Users/clf358/gbif-data/some-issue\",\"TEMP_DWCA_PATH\":\"/Users/clf358/gbif-data/temp\",\"LOCATION\":\"/Users/clf358/gbif-data/location\",\"RAW_OCCURRENCE\":\"/Users/clf358/gbif-data/raw_data\",\"TEMPORAL\":\"/Users/clf358/gbif-data/temporal\",\"LOCATION_ISSUE\":\"/Users/clf358/gbif-data/location_issue\",\"GBIF_BACKBONE\":\"/Users/clf358/gbif-data/gbif-backbone\",\"INTERPRETED_ISSUE\":\"/Users/clf358/gbif-data/interpreted-issue\",\"INTERPRETED_OCURENCE\":\"/Users/clf358/gbif-data/interpreted\",\"VERBATIM\":\"/Users/clf358/gbif-data/verbatim\"}";
    DataFlowPipelineOptions options =
      PipelineOptionsFactory.fromArgs(new String[] {"--datasetId=" + UUID.randomUUID().toString(), "--inputFile=abc123",
        "--targetPaths=" + targetPathMap}).withValidation().as(DataFlowPipelineOptions.class);

    System.out.println(options.getDatasetId());
    System.out.println(options.getInputFile());
    System.out.println(options.getDefaultTargetDirectory());
    System.out.println(options.getTargetPaths());
    System.out.println("HDFS Configuration Directory:" + options.getHDFSConfigurationDirectory());
    Map<Interpretation, String> map = options.getTargetPaths();
    for (Map.Entry<Interpretation, String> entry : map.entrySet()) {
      System.out.println(entry.getKey() + "->" + entry.getValue());
    }
  }

}
