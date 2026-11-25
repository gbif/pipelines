package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ValidatorMetricsPipelineWriteTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  /** The coordinator reads the file as UTF-8 JSON: names outside ASCII must survive. */
  @Test
  public void writesUtf8ReadableJson() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String path = new File(folder.getRoot(), "collect-metrics.json").getAbsolutePath();
    String json = "{\"fileInfos\":[{\"fileName\":\"søer.csv\",\"rowType\":\"Ærø – 中文\"}]}";

    ValidatorMetricsPipeline.writeMetricsJson(fs, path, json);

    JsonNode node;
    try (InputStream in = fs.open(new Path(path))) {
      node = new ObjectMapper().readTree(in);
    }
    assertEquals("søer.csv", node.at("/fileInfos/0/fileName").asText());
    assertEquals("Ærø – 中文", node.at("/fileInfos/0/rowType").asText());
  }
}
