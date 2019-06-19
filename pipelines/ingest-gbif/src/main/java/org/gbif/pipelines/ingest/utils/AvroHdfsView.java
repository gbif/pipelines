package org.gbif.pipelines.ingest.utils;

import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class AvroHdfsView {

  public static Schema avroDefinition(){
    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("OccurrenceRecord")
      .namespace("org.gbif.occurrence.hdfs").fields();
    OccurrenceHDFSTableDefinition.definition().forEach(initializableField -> {
      switch (initializableField.getHiveDataType()) {
        case HiveDataTypes.TYPE_INT:
          builder.name(initializableField.getHiveField()).type().nullable().intType().noDefault();
          break;
        case HiveDataTypes.TYPE_BIGINT:
          builder.name(initializableField.getHiveField()).type().nullable().longType().noDefault();
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
