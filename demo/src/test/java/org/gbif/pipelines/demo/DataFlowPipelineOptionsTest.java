package org.gbif.pipelines.demo;

import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.Interpretation;
import org.gbif.pipelines.core.config.TargetPath;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

public class DataFlowPipelineOptionsTest {

  @Test
  public void testWithDefaultTargetDirectoryAndSettingTargetPathsProgrammatically() {
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs("--datasetId=" + UUID.randomUUID().toString(), "--inputFile=abc123")
        .withValidation()
        .as(DataProcessingPipelineOptions.class);

    String defaultTargetDirectory = "/Users/clf358/gbif/data";
    options.setDefaultTargetDirectory(defaultTargetDirectory);

    Map<Interpretation, TargetPath> interpretationTargetMap = new HashMap<>();
    BiConsumer<Interpretation, String> putInter =
      (i, p) -> interpretationTargetMap.put(i, new TargetPath(defaultTargetDirectory, p));

    putInter.accept(Interpretation.RAW_OCCURRENCE, "raw");
    putInter.accept(Interpretation.INTERPRETED_OCURENCE, "interpreted");
    putInter.accept(Interpretation.TEMPORAL, "temporal");
    putInter.accept(Interpretation.TEMPORAL_ISSUE, "temporal_issue");
    putInter.accept(Interpretation.LOCATION, "location");
    putInter.accept(Interpretation.LOCATION_ISSUE, "location_issue");
    putInter.accept(Interpretation.INTERPRETED_ISSUE, "interpreted");
    putInter.accept(Interpretation.TEMP_DWCA_PATH, "temp_file");

    options.setTargetPaths(interpretationTargetMap);
    options.setDatasetId(UUID.randomUUID().toString());

    System.out.println(options.getDatasetId());
    System.out.println(options.getInputFile());
    System.out.println(options.getDefaultTargetDirectory());
    System.out.println(options.getTargetPaths());
  }

  @Test
  public void testImplicitTargetPaths() throws IOException {
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs("--datasetId=" + UUID.randomUUID().toString(), "--inputFile=abc123")
        .withValidation()
        .as(DataProcessingPipelineOptions.class);

    System.out.println(options.getDatasetId());
    System.out.println(options.getInputFile());
    System.out.println(options.getDefaultTargetDirectory());
    System.out.println(options.getTargetPaths());

    ObjectMapper mapper = new ObjectMapper();
    System.out.println(mapper.writeValueAsString(options.getTargetPaths()));
    options.getTargetPaths().forEach((key, value) -> System.out.println(key + "->" + value));
  }

  @Test
  public void testExplicitTargetPaths() {
    String targetPathMap =
      "{\"TEMPORAL_ISSUE\":\"/Users/clf358/gbif-data/some-issue\",\"TEMP_DWCA_PATH\":\"/Users/clf358/gbif-data/temp\",\"LOCATION\":\"/Users/clf358/gbif-data/location\",\"RAW_OCCURRENCE\":\"/Users/clf358/gbif-data/raw_data\",\"TEMPORAL\":\"/Users/clf358/gbif-data/temporal\",\"LOCATION_ISSUE\":\"/Users/clf358/gbif-data/location_issue\",\"GBIF_BACKBONE\":\"/Users/clf358/gbif-data/gbif-backbone\",\"INTERPRETED_ISSUE\":\"/Users/clf358/gbif-data/interpreted-issue\",\"INTERPRETED_OCURENCE\":\"/Users/clf358/gbif-data/interpreted\",\"VERBATIM\":\"/Users/clf358/gbif-data/verbatim\"}";

    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs("--datasetId=" + UUID.randomUUID().toString(), "--inputFile=abc123", "--targetPaths=" + targetPathMap)
      .withValidation()
      .as(DataProcessingPipelineOptions.class);

    System.out.println(options.getDatasetId());
    System.out.println(options.getInputFile());
    System.out.println(options.getDefaultTargetDirectory());
    System.out.println(options.getTargetPaths());
    //System.out.println("HDFS Configuration Directory:" + options.getHDFSConfigurationDirectory());

    options.getTargetPaths().forEach((key, value) -> System.out.println(key + "->" + value));
  }

}
