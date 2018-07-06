package org.gbif.pipelines.minipipelines.dwca;

import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.StringJoiner;

public class DwcaPipelineTest {

  @Ignore
  @Test
  public void dwcaPipelineTest() {
    final String inputPath =
        getClass().getClassLoader().getResource("dwca.zip").getPath().toString();
    final String targetPath = Paths.get("src", "test", "resources", "output").toString();
    final String gbifEnv = DwcaMiniPipelineOptions.GbifEnv.DEV.name();
    final String datasetId = "abcd1234";
    final String attempt = "1";
    final String ESHosts = "http://localhost:9200";

    StringJoiner joiner = new StringJoiner(" ");
    joiner.add("--inputPath=" + inputPath);
    joiner.add("--targetPath=" + targetPath);
    joiner.add("--gbifEnv=" + gbifEnv);
    joiner.add("--datasetId=" + datasetId);
    joiner.add("--attempt=" + attempt);
    joiner.add("--ESHosts=" + ESHosts);
    joiner.add("--pipelineStep=DWCA_TO_AVRO");

    DwcaPipeline.main(joiner.toString().split(" "));
  }
}
