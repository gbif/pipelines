package org.gbif.pipelines.spark.dwcdp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.gbif.dp.descriptor.DataPackageDescriptor;
import org.gbif.dp.descriptor.DataPackageParser;
import org.gbif.dp.descriptor.DialectDescriptor;
import org.gbif.dp.descriptor.FieldDescriptor;
import org.gbif.dp.descriptor.MissingValueDescriptor;
import org.gbif.dp.descriptor.ResourceDescriptor;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.jspecify.annotations.NonNull;

@Slf4j
public class DataPackageConverter {

  public static final String DATAPACKAGE_SUBDIR = "datapackage";

  private static final Pattern URI_SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*:");
  // Spark interprets Hadoop paths as glob patterns.
  private static final Pattern GLOB_SYNTAX = Pattern.compile("[\\\\*?\\[{]");

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
    Path descriptorPath = getDescriptorPath(source.toAbsolutePath());
    Path sourceBase = descriptorPath.getParent().toRealPath();

    // The shared descriptor parser consumes text independent of the storage backend.
    String descriptorContent = Files.readString(descriptorPath, StandardCharsets.UTF_8);
    DataPackageDescriptor descriptor = parser.parse(descriptorContent);
    JsonNode descriptorTree = mapper.readTree(descriptorContent);
    if (!(descriptorTree instanceof ObjectNode outputDescriptor)) {
      throw new IOException("datapackage.json must contain a JSON object");
    }

    String datapackageDestination =
        (destination.endsWith("/") ? destination : destination + "/") + DATAPACKAGE_SUBDIR;

    List<ResourceConversion> conversions = new ArrayList<>();
    for (ResourceDescriptor resource : descriptor.resources()) {
      if (resource.paths().isEmpty()) {
        log.debug("Skipping resource [{}] without a path", resource.name());
        continue;
      }

      List<String> paths = resource.paths();
      Path first = relativeResourcePath(resource.name(), paths.get(0));
      List<Path> inputs = new ArrayList<>(paths.size());
      inputs.add(resolveInputPath(sourceBase, resource.name(), first));
      for (String path : paths.subList(1, paths.size())) {
        inputs.add(
            resolveInputPath(
                sourceBase, resource.name(), relativeResourcePath(resource.name(), path)));
      }

      String outputRelative = stagedParquetPath(first);
      conversions.add(
          new ResourceConversion(resource, inputs, first.getFileName().toString(), outputRelative));
      updateOutputResource(outputDescriptor, resource.name(), outputRelative);
    }
    validateRewrittenPaths(outputDescriptor);

    Map<String, Long> metrics = new HashMap<>();
    for (ResourceConversion conversion : conversions) {
      String outputUri = datapackageDestination + "/" + conversion.outputRelative();
      long count = readAndWrite(spark, conversion, outputUri);
      metrics.put("COUNT_" + conversion.resource().name().toUpperCase(Locale.ROOT), count);
    }

    metrics.put("COUNT_MAX", metrics.values().stream().mapToLong(Long::longValue).max().orElse(0));

    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(destination);
    FileSystem fs = hadoopPath.getFileSystem(spark.sparkContext().hadoopConfiguration());

    MetricsUtil.writeMetricsYaml(
        fs, metrics, destination + "/" + PipelinesVariables.Pipeline.DWCDP_STAGE + ".yml");

    writeDescriptor(spark, outputDescriptor, datapackageDestination);
  }

  private record ResourceConversion(
      ResourceDescriptor resource,
      List<Path> inputs,
      String inputFilename,
      String outputRelative) {}

  static Path resolveInputPath(Path sourceBase, String resourceName, Path relativePath)
      throws IOException {
    // The NFS package must stay stable for the duration of staging. A path check cannot detect
    // hard links to files outside the package or prevent changes to a directory Spark enumerates.
    Path realInput = sourceBase.resolve(relativePath).toRealPath();
    if (!realInput.startsWith(sourceBase)
        || realInput.equals(sourceBase)
        || GLOB_SYNTAX.matcher(posixPath(realInput)).find()) {
      throw invalidResourcePath(resourceName, posixPath(relativePath));
    }
    if (Files.isDirectory(realInput)) {
      // A Parquet resource can be a directory of part files that Spark enumerates.
      try (var paths = Files.walk(realInput, FileVisitOption.FOLLOW_LINKS)) {
        var links = paths.filter(Files::isSymbolicLink).iterator();
        while (links.hasNext()) {
          if (!links.next().toRealPath().startsWith(sourceBase)) {
            throw invalidResourcePath(resourceName, posixPath(relativePath));
          }
        }
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }
    return realInput;
  }

  private static void validateRewrittenPaths(ObjectNode descriptor) throws IOException {
    // The parser can skip resources that are still present in the original JSON.
    for (JsonNode resource : descriptor.path("resources")) {
      String name = resource.path("name").asText("");
      JsonNode path = resource.path("path");
      Iterable<JsonNode> paths = path.isArray() ? path : List.of(path);
      for (JsonNode entry : paths) {
        String value = entry.asText("");
        if (!value.isBlank()) {
          relativeResourcePath(name.isBlank() ? "(unnamed)" : name, value);
        }
      }
    }
  }

  private static Path relativeResourcePath(String resourceName, String resourcePath)
      throws IOException {
    Path path;
    try {
      path = Path.of(resourcePath);
    } catch (InvalidPathException e) {
      throw new IOException(invalidResourcePath(resourceName, resourcePath).getMessage(), e);
    }
    Path normalized = path.normalize();
    if (path.isAbsolute()
        || normalized.toString().isBlank()
        || URI_SCHEME.matcher(posixPath(normalized)).find()
        || GLOB_SYNTAX.matcher(resourcePath).find()) {
      throw invalidResourcePath(resourceName, resourcePath);
    }
    for (Path segment : path) {
      if (segment.toString().equals("..")) {
        throw invalidResourcePath(resourceName, resourcePath);
      }
    }
    return normalized;
  }

  private static String posixPath(Path path) {
    return path.toString().replace(File.separatorChar, '/');
  }

  private static IOException invalidResourcePath(String resourceName, String resourcePath) {
    return new IOException(
        "Resource ["
            + resourceName
            + "] must use a relative path within the data package: "
            + resourcePath);
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
  }

  private long readAndWrite(SparkSession spark, ResourceConversion conversion, String outputUri) {
    List<Path> inputs = conversion.inputs();

    int partitions = calculatePartitions(inputs, targetPartitionByteSize);

    String[] paths =
        inputs.stream()
            .map(path -> new org.apache.hadoop.fs.Path(path.toUri()).toString())
            .toArray(String[]::new);
    log.debug(
        "Reading local resource paths [{}] => [{}]",
        paths,
        inputs.stream().map(Path::toString).toArray(String[]::new));

    Dataset<Row> df = createReader(spark, conversion.resource(), conversion.inputFilename(), paths);

    df.coalesce(partitions).write().mode(SaveMode.Overwrite).parquet(outputUri);
    return spark.read().parquet(outputUri).count();
  }

  private static Dataset<Row> createReader(
      SparkSession spark, ResourceDescriptor resource, String inputFilename, String[] paths) {
    String filename = inputFilename.toLowerCase(Locale.ROOT);
    if (filename.endsWith(".parquet") || filename.endsWith(".pq")) {
      // The descriptor remains authoritative even when the input is already Parquet. This makes
      // the converted package independent of the publisher's original physical encoding.
      return applyDeclaredTypes(spark.read().parquet(paths), resource);
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

    Dataset<Row> strings = reader.csv(paths);
    return applyDeclaredTypes(strings, resource);
  }

  /**
   * Applies the descriptor's effective per-field missing values before casting to the declared
   * physical type. The descriptor parser has already resolved schema-level missing values into each
   * {@link FieldDescriptor}, so this code does not need to reproduce that precedence logic.
   */
  static Dataset<Row> applyDeclaredTypes(Dataset<Row> df, ResourceDescriptor resource) {
    if (resource.schema() == null || resource.schema().fields().isEmpty()) {
      return df;
    }

    Dataset<Row> typed = df;
    for (FieldDescriptor field : resource.schema().fields()) {
      if (!hasColumn(typed, field.name())) {
        log.warn(
            "Descriptor field [{}] is not present in resource [{}]", field.name(), resource.name());
        continue;
      }

      Column value = typed.col(field.name());
      Column missing = missingValueCondition(value, field.missingValues());
      Column normalized =
          missing == null ? value : functions.when(missing, functions.lit(null)).otherwise(value);

      typed = typed.withColumn(field.name(), normalized.cast(toSparkType(field)));
    }
    return typed;
  }

  private static boolean hasColumn(Dataset<Row> df, String name) {
    for (String column : df.columns()) {
      if (column.equals(name)) {
        return true;
      }
    }
    return false;
  }

  private static Column missingValueCondition(
      Column value, List<MissingValueDescriptor> missingValues) {
    if (missingValues == null || missingValues.isEmpty()) {
      return null;
    }

    Column condition = null;
    for (MissingValueDescriptor missingValue : missingValues) {
      if (missingValue == null || missingValue.rawValue() == null) {
        continue;
      }
      Column match = value.cast(DataTypes.StringType).equalTo(missingValue.rawValue());
      condition = condition == null ? match : condition.or(match);
    }
    return condition;
  }

  static DataType toSparkType(FieldDescriptor field) {
    String type = field.type() == null ? "string" : field.type().toLowerCase(Locale.ROOT);
    return switch (type) {
      case "integer" -> DataTypes.LongType;
      case "number" -> DataTypes.DoubleType;
      case "boolean" -> DataTypes.BooleanType;
      case "date" -> DataTypes.DateType;
      case "datetime" -> DataTypes.TimestampType;
      case "year" -> DataTypes.IntegerType;
        // Spark has no generally useful native equivalents for Frictionless time, yearmonth,
        // duration, geopoint, geojson, object, array, or any in the CSV reader. Preserve their
        // lexical representation rather than inventing a lossy physical encoding here.
      default -> DataTypes.StringType;
    };
  }

  private static void updateOutputResource(
      ObjectNode descriptor, String resourceName, String outputRelative) {
    JsonNode resourcesNode = descriptor.path("resources");
    if (!(resourcesNode instanceof ArrayNode resources)) {
      return;
    }

    for (JsonNode node : resources) {
      if (!(node instanceof ObjectNode resourceNode)) {
        continue;
      }
      if (!resourceName.equals(resourceNode.path("name").asText())) {
        continue;
      }

      // The converted resource is one consolidated Parquet dataset. Keep all unrelated descriptor
      // metadata verbatim, but remove properties whose values described the original byte stream.
      resourceNode.put("path", outputRelative);
      resourceNode.put("format", "parquet");
      resourceNode.remove("dialect");
      resourceNode.remove("encoding");
      resourceNode.remove("bytes");
      resourceNode.remove("hash");
      resourceNode.remove("mediatype");
      return;
    }
  }

  private void writeDescriptor(SparkSession spark, ObjectNode descriptor, String destination)
      throws IOException {
    org.apache.hadoop.fs.Path hadoopPath =
        new org.apache.hadoop.fs.Path(destination + "/datapackage.json");
    FileSystem fs = hadoopPath.getFileSystem(spark.sparkContext().hadoopConfiguration());

    try (OutputStream out = fs.create(hadoopPath, true)) {
      mapper.writerWithDefaultPrettyPrinter().writeValue(out, descriptor);
    }
  }

  private static String stagedParquetPath(Path relativePath) {
    String path = posixPath(relativePath);
    int slash = path.lastIndexOf('/');
    int dot = path.lastIndexOf('.');
    String base = dot > slash ? path.substring(0, dot) : path;
    // HDFS forbids colons in path components. Escape literal percent signs as well.
    return base.replace("%", "%25").replace(":", "%3A") + ".parquet";
  }
}
