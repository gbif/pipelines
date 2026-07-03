package org.gbif.pipelines.spark.dwcdp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageField;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageResource;
import org.gbif.pipelines.spark.dwcdp.model.DataPackageSchema;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds {@link DataPackage} descriptors for use in tests, mirroring the structure that
 * DataPackageConverter writes to {@code datapackage.json}.
 *
 * <p>Paths in the returned descriptors are relative (e.g. {@code data/event.parquet}) and must be
 * combined with the test's base path when constructing a {@link TableLoader}.
 */
class DataPackageFixtures {

  private DataPackageFixtures() {}

  static DataPackage withEvent(String... fieldNames) {
    return build(resource("event", "data/event.parquet", fieldNames));
  }

  static DataPackage withOccurrence(String... fieldNames) {
    String[] fields =
        fieldNames.length > 0 ? fieldNames : new String[] {"occurrenceID", "scientificName"};
    return build(resource("occurrence", "data/occurrence.parquet", fields));
  }

  static DataPackage withEventAndOccurrence() {
    return build(
        resource("event", "data/event.parquet", "eventID"),
        resource(
            "occurrence", "data/occurrence.parquet", "occurrenceID", "eventID", "scientificName"));
  }

  static DataPackage withEventOccurrenceOrganismAndMedia() {
    return build(
        resource(
            "event",
            "data/event.parquet",
            "eventID",
            "parentEventID",
            "eventDate",
            "country",
            "decimalLatitude",
            "decimalLongitude"),
        // associatedOrganisms is not a DwC-DP occurrence field — contributed by organism join
        resource(
            "occurrence",
            "data/occurrence.parquet",
            "occurrenceID",
            "eventID",
            "organismID",
            "scientificName",
            "organismScope",
            "organismName",
            "organismRemarks",
            "occurrenceStatus",
            "sex"),
        resource(
            "organism",
            "data/organism.parquet",
            "organismID",
            "organismName",
            "organismScope",
            "organismRemarks",
            "associatedOrganisms"),
        resource("media", "data/media.parquet", "mediaID", "accessURI", "mediaType"),
        resource(
            "event-media", "data/event-media.parquet",
            "mediaID", "eventID"));
  }

  static DataPackage withOccurrenceOrganismAndMedia() {
    return build(
        // associatedOrganisms is not a DwC-DP occurrence field — contributed by organism join
        resource(
            "occurrence",
            "data/occurrence.parquet",
            "occurrenceID",
            "eventID",
            "organismID",
            "scientificName",
            "organismScope",
            "organismName",
            "organismRemarks",
            "occurrenceStatus",
            "sex",
            "decimalLatitude",
            "decimalLongitude"),
        resource(
            "organism",
            "data/organism.parquet",
            "organismID",
            "organismName",
            "organismScope",
            "organismRemarks",
            "associatedOrganisms"),
        resource("media", "data/media.parquet", "mediaID", "accessURI", "mediaType"),
        resource(
            "occurrence-media", "data/occurrence-media.parquet",
            "mediaID", "occurrenceID"));
  }

  // ---- internals ----

  static DataPackage withEventAndAssertion() {
    return build(
        resource("event", "data/event.parquet", "event_pk", "eventID"),
        resource(
            "event-assertion",
            "data/event-assertion.parquet",
            "assertionID",
            "event_fk",
            "assertionType",
            "assertionValue",
            "assertionUnit"));
  }

  static DataPackage withOccurrenceAndAssertion() {
    return build(
        resource(
            "occurrence",
            "data/occurrence.parquet",
            "occurrence_pk",
            "occurrenceID",
            "scientificName"),
        resource(
            "occurrence-assertion",
            "data/occurrence-assertion.parquet",
            "assertionID",
            "occurrence_fk",
            "assertionType",
            "assertionValue",
            "assertionUnit"));
  }

  static DataPackage withEventAssertionAndProtocol() {
    return build(
        resource("event", "data/event.parquet", "event_pk", "eventID"),
        resource(
            "event-assertion",
            "data/event-assertion.parquet",
            "assertionID",
            "event_fk",
            "assertionType",
            "assertionValue",
            "assertionProtocol_fk"),
        resource("protocol", "data/protocol.parquet", "protocol_pk", "protocolDescription"));
  }

  static DataPackage withEventAndSurvey() {
    return build(
        resource("event", "data/event.parquet", "event_pk", "eventID"),
        resource(
            "survey",
            "data/survey.parquet",
            "survey_pk",
            "event_fk",
            "siteCount",
            "reportedWeather"));
  }

  static DataPackage withEventSurveyAndTarget() {
    return build(
        resource("event", "data/event.parquet", "event_pk", "eventID"),
        resource(
            "survey",
            "data/survey.parquet",
            "survey_pk",
            "event_fk",
            "siteCount",
            "reportedWeather"),
        resource(
            "survey-survey-target", "data/survey-survey-target.parquet",
            "survey_fk", "surveyTarget_fk"),
        resource(
            "survey-target", "data/survey-target.parquet",
            "surveyTarget_pk", "surveyTargetDescription"));
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
