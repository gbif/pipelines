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
import org.apache.hadoop.hdfs.DFSUtilClient;
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
  void stagesFileAliasesAndEscapesColonsInTheDeclaredPath(@TempDir Path dir) throws Exception {
    Path source = Files.createDirectories(dir.resolve("input/dataset"));
    Path target = Files.writeString(source.resolve("source.txt"), "id\tvalue\n1\tkept\n");
    Files.createSymbolicLink(source.resolve("2024-01-01T00:00:00.tsv"), target.getFileName());
    Files.writeString(
        source.resolve("datapackage.json"),
        """
        {"resources":[{"name":"event","path":"2024-01-01T00:00:00.tsv"}]}
        """);

    Path staged = stagePackage(dir);
    String path =
        MapperUtil.MAPPER
            .readTree(Files.readString(staged.resolve("datapackage.json")))
            .path("resources")
            .get(0)
            .path("path")
            .asText();
    assertEquals("2024-01-01T00%3A00%3A00.parquet", path);
    assertTrue(DFSUtilClient.isValidName("/" + path));
    assertTrue(Files.isDirectory(staged.resolve(path)));
    assertEquals(
        "kept", spark.read().parquet("file:" + staged + "/" + path).first().getAs("value"));
  }

  @Test
  void stagesParquetDirectoryWithInPackagePartSymlink(@TempDir Path dir) throws Exception {
    Path source = Files.createDirectories(dir.resolve("input/dataset"));
    Path parts = source.resolve("original");
    spark.range(1, 2).coalesce(1).write().parquet(parts.toUri().toString());
    Path resource = Files.createDirectory(source.resolve("event.parquet"));
    try (var files = Files.list(parts)) {
      Path part =
          files.filter(path -> path.toString().endsWith(".parquet")).findFirst().orElseThrow();
      Files.createSymbolicLink(resource.resolve("part-0.parquet"), resource.relativize(part));
    }
    Files.writeString(
        source.resolve("datapackage.json"),
        """
        {"resources":[{"name":"event","path":"event.parquet"}]}
        """);

    Path staged = stagePackage(dir);
    assertEquals(
        1L,
        spark
            .read()
            .parquet(staged.resolve("event.parquet").toUri().toString())
            .first()
            .getLong(0));
  }

  @Test
  void preservesInlineDataWhenTheParserIgnoresBlankPaths(@TempDir Path dir) throws Exception {
    Path source = Files.createDirectories(dir.resolve("input/dataset"));
    Files.writeString(
        source.resolve("datapackage.json"),
        """
        {"resources":[
          {"name":"inline","data":[{"id":1}]},
          {"name":"blank","path":"","data":[{"id":2}]},
          {"name":"blank-list","path":[""," "],"data":[{"id":3}]}
        ]}
        """);

    Path staged = stagePackage(dir);
    assertEquals(
        MapperUtil.MAPPER.readTree(Files.readString(source.resolve("datapackage.json"))),
        MapperUtil.MAPPER.readTree(Files.readString(staged.resolve("datapackage.json"))));
    try (var files = Files.list(staged)) {
      assertEquals(
          List.of("datapackage.json"),
          files
              .map(path -> path.getFileName().toString())
              .filter(name -> !name.endsWith(".crc"))
              .toList());
    }
  }

  private Path stagePackage(Path dir) throws IOException {
    Path output = dir.resolve("output");
    DataPackageConversionPipeline.runCopy(
        createPipeline(dir.resolve("input").toString(), output.toUri().toString(), "dataset", 0));
    return output.resolve("dataset/0").resolve(DATAPACKAGE_SUBDIR);
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
