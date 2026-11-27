package org.gbif.pipelines.spark;

import static org.gbif.pipelines.spark.dwcdp.DataPackageConverter.DATAPACKAGE_SUBDIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.dp.common.descriptor.JacksonDataPackageParser;
import org.gbif.dp.descriptor.DataPackageDescriptor;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataPackageConversionPipelineTest {

  private static final long TARGET_PARTITION_BYTE_SIZE = 256 * 1024 * 1024;
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

  DataPackageConversionPipeline.CopyConfig createPipeline(
      String inputBasePath, String outputBasePath, String datasetId, int attempt) {
    return new DataPackageConversionPipeline.CopyConfig(
        spark, inputBasePath, outputBasePath, datasetId, attempt, TARGET_PARTITION_BYTE_SIZE);
  }

  @Test
  void tsvPackageIsCopied(@TempDir Path destination) throws Exception {
    String inputBasePath = fixtures.toString();
    String outputBasePath = "file://" + destination;

    DataPackageConversionPipeline.runCopy(
        createPipeline(inputBasePath, outputBasePath, "tsv-package", 0));

    Dataset<Row> df =
        spark
            .read()
            .parquet(
                "file://"
                    + destination
                    + "/tsv-package/0/"
                    + DATAPACKAGE_SUBDIR
                    + "/occurrences.parquet");
    assertEquals(3, df.count());
  }

  @Test
  void outputDescriptorIsWritten(@TempDir Path destination) throws Exception {
    String inputBasePath = fixtures.toString();
    String outputBasePath = "file://" + destination;

    DataPackageConversionPipeline.runCopy(
        createPipeline(inputBasePath, outputBasePath, "tsv-package", 0));

    Path descriptorPath =
        destination
            .resolve("tsv-package/0")
            .resolve(DATAPACKAGE_SUBDIR)
            .resolve("datapackage.json");

    // DataPackageParser is intentionally storage-agnostic: parse descriptor content, not a path.
    DataPackageDescriptor out =
        new JacksonDataPackageParser().parse(Files.readString(descriptorPath));

    assertFalse(out.resources().isEmpty());
    assertNull(out.resources().get(0).dialect());
  }

  @Test
  void rejectsPathsOutsidePackageBeforeWritingAnyResources(@TempDir Path dir) throws Exception {
    Path input = Files.createDirectory(dir.resolve("input"));
    Path source = Files.createDirectory(input.resolve("dataset"));
    Path destination = dir.resolve("output");
    Path outside = Files.writeString(input.resolve("outside.csv"), "id\n1\n");
    Files.writeString(source.resolve("inside.csv"), "id\n2\n");
    Files.createSymbolicLink(source.resolve("link.csv"), outside);
    Path parquet = Files.createDirectory(source.resolve("parts.parquet"));
    Files.createSymbolicLink(parquet.resolve("part-0.parquet"), outside);

    var descriptor = MapperUtil.MAPPER.createObjectNode();
    var resources = descriptor.putArray("resources");
    resources.addObject().put("name", "event").put("path", "inside.csv");
    var occurrence = resources.addObject().put("name", "occurrence");

    for (String invalid :
        List.of(
            "../outside.csv",
            "data/../inside.csv",
            outside.toString(),
            outside.toUri().toString(),
            "link.csv",
            "parts.parquet")) {
      occurrence.putArray("path").add("inside.csv").add(invalid);
      Files.writeString(source.resolve("datapackage.json"), descriptor.toString());

      IOException error =
          assertThrows(
              IOException.class,
              () ->
                  DataPackageConversionPipeline.runCopy(
                      createPipeline(
                          input.toString(), destination.toUri().toString(), "dataset", 0)),
              invalid);
      assertTrue(error.getMessage().contains("Resource [occurrence]"));
      assertTrue(error.getMessage().contains(invalid));
      assertFalse(Files.exists(destination));
    }
  }
}
