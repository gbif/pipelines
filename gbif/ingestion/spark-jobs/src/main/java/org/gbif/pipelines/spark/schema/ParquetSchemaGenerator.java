package org.gbif.pipelines.spark.schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.spark.sql.types.DataType;

/**
 * Writes the published Parquet schemas for the current POJOs into the source tree:
 *
 * <pre>
 *   src/main/resources/schemas/&lt;name&gt;/&lt;version&gt;/&lt;name&gt;.parquet-schema.txt
 *   src/main/resources/schemas/&lt;name&gt;/&lt;version&gt;/&lt;name&gt;.spark-schema.json
 * </pre>
 *
 * <p>Run after changing {@code Occurrence} or {@code Event} and bumping the version in {@link
 * ParquetSchemas}. An already released version is never overwritten.
 *
 * <p>Usage: {@code ParquetSchemaGenerator [outputRoot]}, default {@code src/main/resources}.
 */
public class ParquetSchemaGenerator {

  public static void main(String[] args) throws IOException {
    Path root = Paths.get(args.length > 0 ? args[0] : "src/main/resources");
    for (ParquetSchemas.Published schema : ParquetSchemas.Published.values()) {
      Path message =
          root.resolve(schema.resourcePath(schema.version, ParquetSchemas.PARQUET_SCHEMA_SUFFIX));
      Path spark =
          root.resolve(schema.resourcePath(schema.version, ParquetSchemas.SPARK_SCHEMA_SUFFIX));

      String newMessage = schema.parquetMessage();
      if (Files.exists(message)) {
        String existing = Files.readString(message, StandardCharsets.UTF_8);
        if (!existing.equals(newMessage)) {
          throw new IllegalStateException(
              String.format(
                  "%s %s is already released with a different schema. Bump the version in"
                      + " ParquetSchemas instead of changing a released schema.",
                  schema.schemaName, schema.version));
        }
        System.out.printf("%s %s unchanged%n", schema.schemaName, schema.version);
        continue;
      }

      Files.createDirectories(message.getParent());
      Files.writeString(message, newMessage, StandardCharsets.UTF_8);
      Files.writeString(
          spark,
          DataType.fromJson(schema.sparkSchema().json()).prettyJson() + "\n",
          StandardCharsets.UTF_8);
      System.out.printf("Wrote %s and %s%n", message, spark);
    }
  }
}
