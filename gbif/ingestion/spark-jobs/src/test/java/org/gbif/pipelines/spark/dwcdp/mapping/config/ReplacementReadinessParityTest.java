package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Combined legacy-vs-governing-plan tests used as the replacement-readiness checkpoint. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplacementReadinessParityTest {

  /**
   * Legacy-only nested Occurrence transport fields that are intentionally not part of the
   * governing DwC-A mapping. The JSON columns attempt extension-under-extension transport, while
   * occurrenceProtocol_fk is an unresolved internal surrogate FK leaked by the legacy nested path.
   */
  private static final Set<String> DEFERRED_NESTED_LEGACY_FIELDS =
      Set.of(
          "mediaExtJson",
          "assertionExtJson",
          "identificationExtJson",
          "identifierExtJson",
          "occurrenceProtocol_fk");

  private SparkSession spark;
  private SchemaGraph graph;
  private DwcDpMappingEngine engine;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("ReplacementReadinessParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void occurrenceCurrentPlanMatchesLegacyWithCombinedSupportedData() {
    TableLoader loader = occurrenceLoader();

    List<ComparableRecord> legacy =
        comparable(OccurrenceCoreBuilder.build(spark, loader).collectAsList(), false);
    List<ComparableRecord> mapped =
        comparable(engine.execute(loader, OccurrenceDwcaMapping.current(graph)).collectAsList(), false);

    assertEquals(legacy, mapped);
  }

  @Test
  void eventCurrentPlanMatchesLegacyApartFromDeferredNestedExtensionJson() {
    TableLoader loader = eventLoader();

    List<ComparableRecord> legacy =
        comparable(EventCoreBuilder.build(spark, loader).collectAsList(), true);
    List<ComparableRecord> mapped =
        comparable(engine.execute(loader, EventDwcaMapping.current(graph)).collectAsList(), true);

    assertEquals(legacy, mapped);
  }

  private TableLoader occurrenceLoader() {
    return TestTableLoader.of(
        "occurrence", occurrence(false),
        "agent", agents(),
        "organism", organisms(),
        "identification", identifications(false),
        "material", material(),
        "usage-policy", usagePolicies(),
        "provenance", provenance(),
        "material-provenance", materialProvenance(),
        "material-geological-context", materialGeologicalContext(),
        "geological-context", geologicalContext(),
        "protocol", protocols(),
        "material-protocol", materialProtocol(),
        "media", media(),
        "occurrence-media", occurrenceMedia(),
        "material-media", materialMedia(),
        "occurrence-assertion", occurrenceAssertion(),
        "material-assertion", materialAssertion(),
        "occurrence-identifier", occurrenceIdentifier(),
        "material-identifier", materialIdentifier(),
        "nucleotide-analysis", nucleotideAnalysis(false),
        "nucleotide-sequence", nucleotideSequence(),
        "molecular-protocol", molecularProtocol());
  }

  private TableLoader eventLoader() {
    return TestTableLoader.of(
        "event", event(),
        "agent", agents(),
        "occurrence", occurrence(true),
        "organism", organisms(),
        "identification", identifications(true),
        "material", material(),
        "usage-policy", usagePolicies(),
        "provenance", provenance(),
        "event-provenance", eventProvenance(),
        "material-provenance", materialProvenance(),
        "material-geological-context", materialGeologicalContext(),
        "geological-context", geologicalContext(),
        "protocol", protocols(),
        "event-protocol", eventProtocol(),
        "material-protocol", materialProtocol(),
        "media", media(),
        "event-media", eventMedia(),
        "occurrence-media", occurrenceMedia(),
        "material-media", materialMedia(),
        "event-assertion", eventAssertion(),
        "occurrence-assertion", occurrenceAssertion(),
        "material-assertion", materialAssertion(),
        "event-identifier", eventIdentifier(),
        "occurrence-identifier", occurrenceIdentifier(),
        "material-identifier", materialIdentifier(),
        "nucleotide-analysis", nucleotideAnalysis(true),
        "nucleotide-sequence", nucleotideSequence(),
        "molecular-protocol", molecularProtocol());
  }

  private Dataset<Row> event() {
    return rows(
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("eventProtocol_fk", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("eventConductedByID", DataTypes.StringType)
            .add("georeferencedByID", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType),
        RowFactory.create("EPK-1", "EVT001", "PPK-DIRECT", "GEO-1", "AG-1", "AG-2", "PR-1"));
  }

  private Dataset<Row> occurrence(boolean eventOwned) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType)
            .add("occurrenceProtocol_fk", DataTypes.StringType)
            .add("recordedByID", DataTypes.StringType)
            .add("identifiedByID", DataTypes.StringType)
            .add("recordedBy", DataTypes.StringType)
            .add("identifiedBy", DataTypes.StringType);
    return rows(
        schema,
        RowFactory.create(
            "OPK-1",
            "OCC001",
            eventOwned ? "EPK-1" : null,
            "ORG-1",
            "Occurrence oak",
            "PPK-DIRECT",
            "AG-1",
            "AG-2",
            null,
            null));
  }

  private Dataset<Row> agents() {
    return rows(
        new StructType()
            .add("agentID", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType),
        RowFactory.create("AG-1", "Agent One"),
        RowFactory.create("AG-2", "Agent Two"));
  }

  private Dataset<Row> organisms() {
    return rows(
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType),
        RowFactory.create("ORG-1", "Organism oak", "associated-oak"));
  }

  private Dataset<Row> identifications(boolean eventOwned) {
    return rows(
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("isAcceptedIdentification", DataTypes.BooleanType)
            .add("scientificName", DataTypes.StringType)
            .add("family", DataTypes.StringType),
        RowFactory.create("IDPK-1", "OPK-1", true, "Accepted oak", "Fagaceae"),
        RowFactory.create("IDPK-2", "OPK-1", false, "Historical oak", "Fagaceae"));
  }

  private Dataset<Row> material() {
    return rows(
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType),
        RowFactory.create("MEPK-1", "OCC001", "UP-M", "PR-1", "NHM", "CAT-1"));
  }

  private Dataset<Row> usagePolicies() {
    return rows(
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType),
        RowFactory.create("UP-M", "CC_BY_4_0", "Museum X"),
        RowFactory.create("UP-MEDIA", "CC0_1_0", "Photographer Y"));
  }

  private Dataset<Row> provenance() {
    return rows(
        new StructType()
            .add("provenance_pk", DataTypes.StringType)
            .add("provenanceID", DataTypes.StringType)
            .add("fundingAttribution", DataTypes.StringType)
            .add("fundingAttributionID", DataTypes.StringType)
            .add("projectID", DataTypes.StringType)
            .add("projectTitle", DataTypes.StringType),
        RowFactory.create("PR-1", "A", "Fund A", "F-A", "P-A", "Project A"),
        RowFactory.create("PR-2", "B", "Fund B", "F-B", "P-B", "Project B"));
  }

  private Dataset<Row> materialProvenance() {
    return rows(
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType),
        RowFactory.create("MEPK-1", "PR-2"));
  }

  private Dataset<Row> eventProvenance() {
    return rows(
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType),
        RowFactory.create("EPK-1", "PR-2"));
  }

  private Dataset<Row> materialGeologicalContext() {
    return rows(
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("geologicalContext_fk", DataTypes.StringType),
        RowFactory.create("MEPK-1", "GCPK-1"));
  }

  private Dataset<Row> geologicalContext() {
    return rows(
        new StructType()
            .add("geologicalContext_pk", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("formation", DataTypes.StringType)
            .add("bed", DataTypes.StringType),
        RowFactory.create("GCPK-1", "GEO-1", "Morrison", "Brushy Basin"));
  }

  private Dataset<Row> protocols() {
    return rows(
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolType", DataTypes.StringType)
            .add("protocolName", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType),
        RowFactory.create("PPK-DIRECT", "sampling", "Direct", "Direct description"),
        RowFactory.create("PPK-MAT", "sampling", "Material", "Material description"));
  }

  private Dataset<Row> materialProtocol() {
    return rows(
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("protocol_fk", DataTypes.StringType),
        RowFactory.create("MEPK-1", "PPK-MAT"));
  }

  private Dataset<Row> eventProtocol() {
    return rows(
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("protocol_fk", DataTypes.StringType),
        RowFactory.create("EPK-1", "PPK-MAT"));
  }

  private Dataset<Row> media() {
    return rows(
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType),
        RowFactory.create("MPK-E", "UP-MEDIA", "https://example.org/event.jpg"),
        RowFactory.create("MPK-O", "UP-MEDIA", "https://example.org/occurrence.jpg"),
        RowFactory.create("MPK-M", "UP-MEDIA", "https://example.org/material.jpg"));
  }

  private Dataset<Row> eventMedia() {
    return rows(
        new StructType().add("event_fk", DataTypes.StringType).add("media_fk", DataTypes.StringType),
        RowFactory.create("EPK-1", "MPK-E"));
  }

  private Dataset<Row> occurrenceMedia() {
    return rows(
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType),
        RowFactory.create("OPK-1", "MPK-O"));
  }

  private Dataset<Row> materialMedia() {
    return rows(
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType),
        RowFactory.create("MEPK-1", "MPK-M"));
  }

  private StructType assertionSchema(String parentFk) {
    return new StructType()
        .add("assertionID", DataTypes.StringType)
        .add(parentFk, DataTypes.StringType)
        .add("assertionType", DataTypes.StringType)
        .add("assertionTypeIRI", DataTypes.StringType)
        .add("assertionValue", DataTypes.StringType)
        .add("assertionValueIRI", DataTypes.StringType)
        .add("assertionUnit", DataTypes.StringType)
        .add("assertionUnitIRI", DataTypes.StringType)
        .add("assertionError", DataTypes.StringType)
        .add("assertionBy", DataTypes.StringType)
        .add("assertionMadeDate", DataTypes.StringType)
        .add("assertionRemarks", DataTypes.StringType)
        .add("assertionProtocol_fk", DataTypes.StringType);
  }

  private Row assertion(String id, String parent) {
    return RowFactory.create(
        id, parent, "length", null, "12", null, "mm", null, null, null, null, null, null);
  }

  private Dataset<Row> eventAssertion() {
    return rows(assertionSchema("event_fk"), assertion("AE-1", "EPK-1"));
  }

  private Dataset<Row> occurrenceAssertion() {
    return rows(assertionSchema("occurrence_fk"), assertion("AO-1", "OPK-1"));
  }

  private Dataset<Row> materialAssertion() {
    return rows(assertionSchema("materialEntity_fk"), assertion("AM-1", "MEPK-1"));
  }

  private Dataset<Row> eventIdentifier() {
    return rows(
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType),
        RowFactory.create("EPK-1", "EVENT-ALT", "local"));
  }

  private Dataset<Row> occurrenceIdentifier() {
    return rows(
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType),
        RowFactory.create("OPK-1", "OCC-ALT", "local"));
  }

  private Dataset<Row> materialIdentifier() {
    return rows(
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType),
        RowFactory.create("MEPK-1", "MAT-ALT", "local"));
  }

  private Dataset<Row> nucleotideAnalysis(boolean eventOwned) {
    return rows(
        new StructType()
            .add("nucleotideAnalysis_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("molecularProtocol_fk", DataTypes.StringType)
            .add("nucleotideSequence_fk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("readCount", DataTypes.IntegerType),
        eventOwned
            ? RowFactory.create("NAPK-E", "EPK-1", "MOLP-1", "NSPK-1", null, 42)
            : RowFactory.create("NAPK-O", null, "MOLP-1", "NSPK-1", "MEPK-1", 42));
  }

  private Dataset<Row> nucleotideSequence() {
    return rows(
        new StructType()
            .add("nucleotideSequence_pk", DataTypes.StringType)
            .add("nucleotideSequenceID", DataTypes.StringType)
            .add("sequence", DataTypes.StringType)
            .add("nucleotideSequenceRemarks", DataTypes.StringType),
        RowFactory.create("NSPK-1", "SEQ-1", "GGCCTTAA", "clean"));
  }

  private Dataset<Row> molecularProtocol() {
    return rows(
        new StructType()
            .add("molecularProtocol_pk", DataTypes.StringType)
            .add("target_gene", DataTypes.StringType)
            .add("pcr_analysis_software", DataTypes.StringType),
        RowFactory.create("MOLP-1", "COI", "tool-x"));
  }

  private Dataset<Row> rows(StructType schema, Row... rows) {
    return spark.createDataFrame(List.of(rows), schema);
  }

  private List<ComparableRecord> comparable(List<ExtendedRecord> records, boolean nestedEventMode) {
    return records.stream()
        .map(record -> comparable(record, nestedEventMode))
        .sorted(Comparator.comparing(ComparableRecord::id))
        .toList();
  }

  private ComparableRecord comparable(ExtendedRecord record, boolean nestedEventMode) {
    Map<String, String> core = new TreeMap<>(record.getCoreTerms());
    Map<String, List<Map<String, String>>> extensions = new TreeMap<>();
    if (record.getExtensions() != null) {
      record.getExtensions().forEach(
          (rowType, rows) -> {
            List<Map<String, String>> normalized = new ArrayList<>();
            for (Map<String, String> row : rows) {
              Map<String, String> copy = new LinkedHashMap<>(row);
              copy.remove(TargetTerms.resolve("eventID"));
              copy.remove(TargetTerms.resolve("occurrenceID"));
              if (nestedEventMode && rowType.equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE)) {
                DEFERRED_NESTED_LEGACY_FIELDS.forEach(copy::remove);
              }
              normalized.add(new TreeMap<>(copy));
            }
            normalized.sort(Comparator.comparing(Map::toString));
            if (!normalized.isEmpty()) {
              extensions.put(rowType, normalized);
            }
          });
    }
    return new ComparableRecord(record.getId(), record.getCoreRowType(), core, extensions);
  }

  private record ComparableRecord(
      String id,
      String coreRowType,
      Map<String, String> coreTerms,
      Map<String, List<Map<String, String>>> extensions) {}
}
