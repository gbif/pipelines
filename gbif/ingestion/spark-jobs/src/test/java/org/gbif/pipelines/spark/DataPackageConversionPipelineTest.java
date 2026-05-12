package org.gbif.pipelines.spark;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.dp.descriptor.DataPackageDescriptor;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataPackageConversionPipelineTest {

  SparkSession spark;
  Path fixtures;

  @BeforeAll
  void setup() throws URISyntaxException {
    spark = SparkTestSession.createBuilder().appName("copy-pipeline-test").getOrCreate();
    fixtures = Paths.get(getClass().getClassLoader().getResource("fixtures").toURI());
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void tsvPackageIsCopied(@TempDir Path destination) throws Exception {
    PipelinesConfig config =
        minimalConfig(fixtures.resolve("tsv-package").toString(), "file://" + destination);

    DataPackageConversionPipeline.runCopy(spark, config, "tsv-package", 0);

    Dataset<Row> df =
        spark.read().parquet("file://" + destination + "/tsv-package/0/occurrences.parquet");
    assertEquals(3, df.count());
  }

  @Test
  void outputDescriptorIsWritten(@TempDir Path destination) throws Exception {
    PipelinesConfig config =
        minimalConfig(fixtures.resolve("tsv-package").toString(), "file://" + destination);

    DataPackageConversionPipeline.runCopy(spark, config, "tsv-package", 0);

    DataPackageDescriptor out =
        new JacksonDataPackageParser().parse(destination.resolve("tsv-package/0/datapackage.json"));

    assertFalse(out.resources().isEmpty());
    assertNull(out.resources().get(0).dialect());
  }

  private static PipelinesConfig minimalConfig(String inputPath, String outputPath) {
    PipelinesConfig config = new PipelinesConfig();
    config.setInputPath(inputPath);
    config.setOutputPath(outputPath);
    return config;
  }
}
