package org.gbif.pipelines.spark.dwcdp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.gbif.dp.descriptor.*;
import org.jspecify.annotations.NonNull;

@Slf4j
public class DataPackageConverter {

  private final DataPackageParser parser;
  private final ObjectMapper mapper;
  private final long targetPartitionByteSize;

  public DataPackageConverter(
      DataPackageParser parser, ObjectMapper mapper, long targetPartitionByteSize) {
    this.parser = parser;
    this.mapper = mapper;
    this.targetPartitionByteSize = targetPartitionByteSize;
  }

  public void convert(SparkSession spark, Path source, String destination) throws IOException {
    Path descriptorPath = getDescriptorPath(source);
    Path sourceBase = descriptorPath.getParent();

    DataPackageDescriptor descriptor = parser.parse(descriptorPath);
    List<ResourceDescriptor> converted = new ArrayList<>();

    for (ResourceDescriptor resource : descriptor.resources()) {
      Path inputAbsolute = resource.paths().get(0);
      Path inputRelative = sourceBase.relativize(inputAbsolute);

      String outputRelative = swapExtension(inputRelative.toString(), "parquet");
      String outputUri = destination + "/" + outputRelative;

      readAndWrite(spark, resource, resource.paths(), outputUri);

      converted.add(
          new ResourceDescriptor(
              resource.name(),
              List.of(Path.of(outputRelative)),
              resource.fields(),
              resource.foreignKeys(),
              resource.primaryKey(),
              null));
    }

    DataPackageDescriptor outputDescriptor =
        new DataPackageDescriptor(descriptor.name(), converted);
    writeDescriptor(spark, outputDescriptor, destination);
  }

  private static @NonNull Path getDescriptorPath(Path source) {
    if (!Files.isDirectory(source)) {
      log.debug("Source path is not a directory: {}", source);
      if (source.toString().toLowerCase(Locale.ROOT).endsWith("datapackage.json")) {
        return source;
      }
      throw new RuntimeException(
          String.format(
              "Source was %s, expected either folder for 'datapackage.json, or directly datapackage.json path",
              source));
    }
    Path resolved = source.resolve("datapackage.json");
    if (!Files.exists(resolved)) {
      throw new RuntimeException(
          String.format(
              "Tried resolving %s from %s, but %s does not exist, expected datapackage.json at root of datasetId directory",
              resolved, source, resolved));
    }
    return resolved;
  }

  public static int calculatePartitions(List<Path> paths, long targetPartitionBytes) {
    long totalBytes =
        paths.stream()
            .filter(Files::exists)
            .filter(Files::isRegularFile)
            .mapToLong(
                path -> {
                  try {
                    return Files.size(path);
                  } catch (IOException e) {
                    throw new RuntimeException("Unable to get size for: " + path, e);
                  }
                })
            .sum();
    long partitions = (totalBytes + targetPartitionBytes - 1) / targetPartitionBytes;
    return Math.max(1, (int) partitions);

  private void readAndWrite(
      SparkSession spark, ResourceDescriptor resource, List<Path> inputs, String outputUri) {

    int partitions = calculatePartitions(inputs, targetPartitionByteSize);

    String[] paths =
        inputs.stream().map(Path::toString).map(p -> "file://" + p).toArray(String[]::new);
    log.debug(
        "Added hdfs file protocol [{}] => [{}]",
        paths,
        inputs.stream().map(Path::toString).toArray(String[]::new));

    Dataset<Row> df = createReader(spark, resource, inputs, paths);

    df.coalesce(partitions).write().mode(SaveMode.Overwrite).parquet(outputUri);
  }

  private static Dataset<Row> createReader(
      SparkSession spark, ResourceDescriptor resource, List<Path> inputs, String[] paths) {
    String filename = inputs.get(0).getFileName().toString();
    if (filename.endsWith(".parquet")) {
      return spark.read().parquet(paths);
    }

    DialectDescriptor dialect =
        resource.dialect() != null ? resource.dialect() : DialectDescriptor.fromExtension(filename);

    DataFrameReader reader =
        spark
            .read()
            .option("header", true)
            .option("delimiter", dialect.delimiter())
            .option("inferSchema", false);

    if (dialect.quoteChar() != null && !dialect.quoteChar().isEmpty()) {
      reader = reader.option("quote", dialect.quoteChar());
    }
    if (dialect.escapeChar() != null) {
      reader = reader.option("escape", dialect.escapeChar());
    }
    if (dialect.nullSequence() != null) {
      reader = reader.option("nullValue", dialect.nullSequence());
    }
    if (dialect.skipInitialSpace()) {
      reader = reader.option("ignoreLeadingWhiteSpace", true);
    }

    return reader.csv(paths);
  }

  private void writeDescriptor(
      SparkSession spark, DataPackageDescriptor descriptor, String destination) throws IOException {
    org.apache.hadoop.fs.Path hadoopPath =
        new org.apache.hadoop.fs.Path(destination + "/datapackage.json");
    FileSystem fs = hadoopPath.getFileSystem(spark.sparkContext().hadoopConfiguration());

    ObjectNode root = mapper.createObjectNode();
    root.put("name", descriptor.name());

    ArrayNode resources = root.putArray("resources");
    for (ResourceDescriptor resource : descriptor.resources()) {
      ObjectNode r = resources.addObject();
      r.put("name", resource.name());

      List<Path> paths = resource.paths();
      if (paths.size() == 1) {
        r.put("path", paths.get(0).toString());
      } else {
        ArrayNode pathArray = r.putArray("path");
        paths.forEach(p -> pathArray.add(p.toString()));
      }

      if (!resource.fields().isEmpty()) {
        ObjectNode schema = r.putObject("schema");

        ArrayNode fields = schema.putArray("fields");
        for (FieldDescriptor field : resource.fields()) {
          ObjectNode f = fields.addObject();
          f.put("name", field.name());
          f.put("type", field.type());
          if (field.format() != null && !field.format().equals("default")) {
            f.put("format", field.format());
          }
        }

        if (resource.primaryKey() != null) {
          List<String> keys = resource.primaryKey().keys();
          if (keys.size() == 1) {
            schema.put("primaryKey", keys.get(0));
          } else {
            ArrayNode pk = schema.putArray("primaryKey");
            keys.forEach(pk::add);
          }
        }

        if (!resource.foreignKeys().isEmpty()) {
          ArrayNode fks = schema.putArray("foreignKeys");
          for (ForeignKeyDescriptor fk : resource.foreignKeys()) {
            ObjectNode fkNode = fks.addObject();
            ArrayNode fkFields = fkNode.putArray("fields");
            fk.fields().forEach(fkFields::add);
            ObjectNode ref = fkNode.putObject("reference");
            ref.put("resource", fk.reference().resource());
            ArrayNode refFields = ref.putArray("fields");
            fk.reference().fields().forEach(refFields::add);
          }
        }
      }
    }

    try (OutputStream out = fs.create(hadoopPath, true)) {
      mapper.writerWithDefaultPrettyPrinter().writeValue(out, root);
    }
  }

  private static String swapExtension(String path, String newExtension) {
    int dot = path.lastIndexOf('.');
    String base = dot >= 0 ? path.substring(0, dot) : path;
    return base + "." + newExtension;
  }
}
