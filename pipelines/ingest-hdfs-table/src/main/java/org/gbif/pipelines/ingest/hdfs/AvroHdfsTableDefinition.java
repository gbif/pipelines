package org.gbif.pipelines.ingest.hdfs;

import lombok.experimental.UtilityClass;
import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

@SuppressWarnings("FallThrough")
@UtilityClass
/**
 * Utility class to generate an Avro scheme from the OccurrenceHdfs Table schema.
 */
public class AvroHdfsTableDefinition {

  /**
   * Generates an Avro Schema based on the Occurrence HDFS table.
   */
  public static Schema avroDefinition() {
    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("OccurrenceHdfsRecord")
      .namespace("org.gbif.pipelines.io.avro").fields();
    OccurrenceHDFSTableDefinition.definition().forEach(initializableField -> {
      switch (initializableField.getHiveDataType()) {
        case HiveDataTypes.TYPE_INT:
          builder.name(initializableField.getHiveField()).type().nullable().intType().noDefault();
          break;
        case HiveDataTypes.TYPE_BIGINT:
          if (initializableField.getHiveField().equalsIgnoreCase("gbifid")) {
            builder.name(initializableField.getHiveField()).type().longType().noDefault();
          } else {
            builder.name(initializableField.getHiveField()).type().nullable().longType().noDefault();
          }
          break;
        case HiveDataTypes.TYPE_BOOLEAN:
          builder.name(initializableField.getHiveField()).type().nullable().booleanType().noDefault();
          break;
        case HiveDataTypes.TYPE_DOUBLE:
          builder.name(initializableField.getHiveField()).type().nullable().doubleType().noDefault();
          break;
        case HiveDataTypes.TYPE_ARRAY_STRING:
          builder.name(initializableField.getHiveField()).type().nullable().array().items().nullable().stringType().noDefault();
          break;
        default:
          builder.name(initializableField.getHiveField()).type().nullable().stringType().noDefault();
          break;
      }
    });
    return builder.endRecord();
  }
}
