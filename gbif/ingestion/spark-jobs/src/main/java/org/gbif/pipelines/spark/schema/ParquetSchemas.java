package org.gbif.pipelines.spark.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.pojo.Event;
import org.gbif.pipelines.spark.pojo.Occurrence;

/**
 * The published Parquet schemas for interpreted occurrences and events.
 *
 * <p>The {@link Occurrence} and {@link Event} POJOs are the definition. Their Parquet schema is
 * derived from them with Spark's bean encoder, so it is exactly what the pipelines write. The
 * released schemas are checked in under {@code src/main/resources/schemas/<name>/<version>/} and
 * {@code ParquetSchemasTest} fails if a POJO changes without a matching version and regenerated
 * files (run {@link ParquetSchemaGenerator}).
 *
 * <p>Versioning (semantic versioning, per schema, independent of the pipelines release):
 *
 * <ul>
 *   <li>Patch: documentation only.
 *   <li>Minor: additive only, i.e. new optional fields. Every field of earlier versions with the
 *       same major keeps its name and type (checked by {@code ParquetSchemasTest}).
 *   <li>Major: anything else (removing, renaming or retyping a field, changing its meaning).
 * </ul>
 *
 * <p>Every output directory carries a {@value #DESCRIPTOR_FILE} file naming the schema and version.
 * Spark and other readers ignore files starting with an underscore.
 */
@Slf4j
public final class ParquetSchemas {

  public static final String OCCURRENCE = "occurrence";
  public static final String OCCURRENCE_VERSION = "1.0.0";

  public static final String EVENT = "event";
  public static final String EVENT_VERSION = "1.0.0";

  /** Schema descriptor written into every Parquet output directory. */
  public static final String DESCRIPTOR_FILE = "_gbif_schema.json";

  /** Classpath root of the checked-in published schemas. */
  public static final String RESOURCE_ROOT = "schemas";

  public static final String PARQUET_SCHEMA_SUFFIX = ".parquet-schema.txt";
  public static final String SPARK_SCHEMA_SUFFIX = ".spark-schema.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ParquetSchemas() {}

  /** A published schema: its name, version and the POJO that defines it. */
  public enum Published {
    OCCURRENCE_SCHEMA(OCCURRENCE, OCCURRENCE_VERSION, Occurrence.class),
    EVENT_SCHEMA(EVENT, EVENT_VERSION, Event.class);

    public final String schemaName;
    public final String version;
    public final Class<?> pojo;

    Published(String schemaName, String version, Class<?> pojo) {
      this.schemaName = schemaName;
      this.version = version;
      this.pojo = pojo;
    }

    /** The Spark schema written by the bean encoder for the POJO. */
    public StructType sparkSchema() {
      return Encoders.bean(pojo).schema();
    }

    /** The Parquet message definition, e.g. {@code message occurrence { ... }}. */
    public String parquetMessage() {
      return toParquetMessage(schemaName, sparkSchema());
    }

    /** Path of a checked-in schema file, relative to the classpath root. */
    public String resourcePath(String version, String suffix) {
      return RESOURCE_ROOT + "/" + schemaName + "/" + version + "/" + schemaName + suffix;
    }
  }

  /** Converts a Spark schema to the Parquet message Spark writes for it, named {@code name}. */
  public static String toParquetMessage(String name, StructType schema) {
    MessageType converted = new SparkToParquetSchemaConverter(new SQLConf()).convert(schema);
    return new MessageType(name, converted.getFields()).toString();
  }

  /**
   * Writes the schema descriptor ({@value #DESCRIPTOR_FILE}) into a Parquet output directory, so
   * readers can tell which schema and version the files follow without opening them.
   */
  public static void writeDescriptor(SparkSession spark, String directory, Published schema) {
    try {
      Path path = new Path(directory, DESCRIPTOR_FILE);
      FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
      try (OutputStream out = fs.create(path, true)) {
        out.write(descriptorJson(schema).getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      // the descriptor is informative; the Parquet output is still valid without it
      log.warn("Could not write schema descriptor to {}", directory, e);
    }
  }

  /** The descriptor content: schema name, version, Parquet message and Spark schema. */
  public static String descriptorJson(Published schema) throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("name", schema.schemaName);
    node.put("version", schema.version);
    node.put("parquetSchema", schema.parquetMessage());
    node.set("sparkSchema", MAPPER.readTree(schema.sparkSchema().json()));
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node);
  }
}
