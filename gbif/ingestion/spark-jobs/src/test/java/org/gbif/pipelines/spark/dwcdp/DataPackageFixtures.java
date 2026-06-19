package org.gbif.pipelines.spark.dwcdp;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter.DataPackage;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter.DataPackageField;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter.DataPackageResource;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter.DataPackageSchema;

/**
 * Builds {@link DataPackage} descriptors for use in tests, mirroring the structure that
 * DataPackageConverter writes to {@code datapackage.json}.
 */
class DataPackageFixtures {

  private DataPackageFixtures() {}

  static DataPackage withEvent(Path dir, String... fieldNames) {
    return build(resource("event", "data/event.parquet", fieldNames));
  }

  static DataPackage withOccurrence(Path dir, String... fieldNames) {
    String[] fields =
        fieldNames.length > 0 ? fieldNames : new String[] {"occurrenceID", "scientificName"};
    return build(resource("occurrence", "data/occurrence.parquet", fields));
  }

  static DataPackage withEventAndOccurrence(Path dir) {
    return build(
        resource("event", "data/event.parquet", "eventID"),
        resource(
            "occurrence", "data/occurrence.parquet", "occurrenceID", "eventID", "scientificName"));
  }

  static DataPackage withEventAndMedia(Path dir) {
    return build(
        resource("event", "data/event.parquet", "eventID"),
        resource("media", "data/media.parquet", "mediaID", "accessURI", "mediaType"),
        resource("event-media", "data/event-media.parquet", "mediaID", "eventID"));
  }

  static DataPackage withEventOccurrenceAndMedia(Path dir) {
    return build(
        resource("event", "data/event.parquet", "eventID"),
        resource(
            "occurrence", "data/occurrence.parquet", "occurrenceID", "eventID", "scientificName"),
        resource("media", "data/media.parquet", "mediaID", "accessURI", "mediaType"),
        resource("event-media", "data/event-media.parquet", "mediaID", "eventID"));
  }

  static DataPackage withOccurrenceAndMedia(Path dir) {
    return build(
        resource("occurrence", "data/occurrence.parquet", "occurrenceID", "scientificName"),
        resource("media", "data/media.parquet", "mediaID", "accessURI", "mediaType"),
        resource("occurrence-media", "data/occurrence-media.parquet", "mediaID", "occurrenceID"));
  }

  private static DataPackage build(DataPackageResource... resources) {
    DataPackage dp = new DataPackage();
    dp.setResources(new ArrayList<>(Arrays.asList(resources)));
    return dp;
  }

  private static DataPackageResource resource(String name, String path, String... fieldNames) {
    DataPackageResource r = new DataPackageResource();
    r.setName(name);
    r.setPath(path);
    DataPackageSchema schema = new DataPackageSchema();
    schema.setFields(
        Arrays.stream(fieldNames).map(DataPackageFixtures::field).collect(Collectors.toList()));
    r.setSchema(schema);
    return r;
  }

  private static DataPackageField field(String name) {
    DataPackageField f = new DataPackageField();
    f.setName(name);
    f.setType("string");
    return f;
  }
}
