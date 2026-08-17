package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingDecision;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingDecisionType;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OccurrenceMappingParityTest {

  private SparkSession spark;
  private SchemaGraph graph;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OccurrenceMappingParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = EventDwcaMapping.withOccurrenceExtension(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void governingMappingUsesEventFkScopeAndOccurrencePkRowIdentity() {
    CompiledMapping compiled = engine.compile(plan);
    CompiledExtension extension =
        compiled.extensions().stream()
            .filter(e -> e.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .findFirst()
            .orElseThrow();
    CompiledFragment fragment =
        extension.fragments().stream()
            .filter(f -> f.name().equals("occurrence-direct"))
            .findFirst()
            .orElseThrow();
    CompiledFragment organism =
        extension.fragments().stream()
            .filter(f -> f.name().equals("occurrence-organism"))
            .findFirst()
            .orElseThrow();

    assertEquals("occurrence.event_fk", fragment.scopeKey().qualifiedName());
    assertEquals(
        "occurrence.occurrence_pk", fragment.rowIdentity().orElseThrow().qualifiedName());
    assertEquals("occurrence.event_fk", organism.scopeKey().qualifiedName());
    assertTrue(organism.rowIdentity().isEmpty());
    assertEquals(
        "occurrence.occurrence_pk", organism.rowMatch().orElseThrow().qualifiedName());
    SchemaPath occurrencePath = SchemaPath.root("occurrence");
    assertTrue(
        organism.targets().stream()
            .flatMap(target -> target.sources().stream())
            .allMatch(
                source ->
                    source.field().path().equals(organism.path())
                        || source.field().path().equals(occurrencePath)),
        "organism targets may source from the organism path or direct occurrence path for "
            + "column-presence precedence");
    assertTrue(
        organism.targets().stream()
            .filter(target -> target.sources().size() > 1)
            .allMatch(
                target ->
                    target.sources().stream()
                        .map(source -> source.field().path())
                        .collect(java.util.stream.Collectors.toSet())
                        .equals(java.util.Set.of(occurrencePath, organism.path()))),
        "overlapping organism targets must explicitly carry both occurrence and organism paths");
  }

  @Test
  void twoOccurrencesUnderOneEventRemainTwoExtensionRowsAndMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(RowFactory.create("EPK-1", "EVT001")),
            "occurrence",
            occurrences(
                RowFactory.create("OPK-1", "OCC001", "EPK-1", "Quercus robur"),
                RowFactory.create("OPK-2", "OCC002", "EPK-1", "Pinus sylvestris")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    List<Map<String, String>> legacyRows =
        legacy.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);
    List<Map<String, String>> mappedRows =
        mapped.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);

    assertNotNull(mappedRows);
    assertEquals(legacyRows.size(), mappedRows.size());
    assertEquals(2, mappedRows.size());

    List<Map<String, String>> sorted =
        mappedRows.stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();
    assertEquals("OCC001", sorted.get(0).get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("Quercus robur", sorted.get(0).get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("OCC002", sorted.get(1).get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("Pinus sylvestris", sorted.get(1).get(DwcTerm.scientificName.qualifiedName()));
    assertFalse(sorted.get(0).containsKey("occurrence_pk"));
    assertFalse(sorted.get(0).containsKey("event_fk"));
  }

  @Test
  void occurrencesAttachToTheirOwnEventsThroughSurrogateEventFk() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(
                RowFactory.create("EPK-1", "EVT001"),
                RowFactory.create("EPK-2", "EVT002")),
            "occurrence",
            occurrences(
                RowFactory.create("OPK-1", "OCC001", "EPK-2", "Pinus sylvestris"),
                RowFactory.create("OPK-2", "OCC002", "EPK-1", "Quercus robur")));

    List<ExtendedRecord> records = engine.execute(loader, plan).collectAsList();
    ExtendedRecord first = byId(records, "EVT001");
    ExtendedRecord second = byId(records, "EVT002");

    assertEquals(
        "OCC002",
        first
            .getExtensions()
            .get(OccurrenceMapping.ROW_TYPE_OCCURRENCE)
            .get(0)
            .get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals(
        "OCC001",
        second
            .getExtensions()
            .get(OccurrenceMapping.ROW_TYPE_OCCURRENCE)
            .get(0)
            .get(DwcTerm.occurrenceID.qualifiedName()));
  }

  @Test
  void organismEnrichmentStaysMatchedToItsOwnOccurrenceAndOccurrenceValuesWin() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(RowFactory.create("EPK-1", "EVT001")),
            "occurrence",
            occurrencesWithOrganism(
                RowFactory.create(
                    "OPK-1", "OCC001", "EPK-1", "ORG-1", "Occurrence oak", "Quercus robur"),
                RowFactory.create(
                    "OPK-2", "OCC002", "EPK-1", "ORG-2", null, "Pinus sylvestris")),
            "organism",
            organisms(
                RowFactory.create("ORG-1", "Organism oak", "oak-associate"),
                RowFactory.create("ORG-2", "Organism pine", "pine-associate")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    List<Map<String, String>> legacyRows =
        legacy.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);
    List<Map<String, String>> mappedRows =
        mapped.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);

    assertEquals(legacyRows.size(), mappedRows.size());
    List<Map<String, String>> sortedLegacy =
        legacyRows.stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();
    List<Map<String, String>> sortedMapped =
        mappedRows.stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();

    String organismName = TargetTerms.resolve("organismName");
    String associatedOrganisms = TargetTerms.resolve("associatedOrganisms");

    assertEquals("Occurrence oak", sortedLegacy.get(0).get(organismName));
    assertEquals("Occurrence oak", sortedMapped.get(0).get(organismName));
    assertEquals("oak-associate", sortedLegacy.get(0).get(associatedOrganisms));
    assertEquals("oak-associate", sortedMapped.get(0).get(associatedOrganisms));

    // Legacy precedence is column-based, not value-based: once occurrence declares organismName,
    // organism.organismName is excluded from the enrichment even when the occurrence value is null.
    assertNull(sortedLegacy.get(1).get(organismName));
    assertNull(sortedMapped.get(1).get(organismName));
    assertEquals("pine-associate", sortedLegacy.get(1).get(associatedOrganisms));
    assertEquals("pine-associate", sortedMapped.get(1).get(associatedOrganisms));
  }

  @Test
  void acceptedIdentificationEnrichesOnlyExactlyOneAcceptedRowAndStaysMatched() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(RowFactory.create("EPK-1", "EVT001")),
            "occurrence",
            occurrencesForIdentification(
                RowFactory.create("OPK-1", "OCC001", "EPK-1", "Occurrence oak"),
                RowFactory.create("OPK-2", "OCC002", "EPK-1", "Occurrence pine"),
                RowFactory.create("OPK-3", "OCC003", "EPK-1", "Occurrence birch")),
            "identification",
            identifications(
                RowFactory.create("ID-1", "OPK-1", true, "Identification oak", "Plantae", "Fagaceae"),
                RowFactory.create("ID-2", "OPK-1", false, "Old oak", "Plantae", "Fagaceae"),
                RowFactory.create("ID-3", "OPK-2", true, "Identification pine A", "Plantae", "Pinaceae"),
                RowFactory.create("ID-4", "OPK-2", true, "Identification pine B", "Plantae", "Pinaceae"),
                RowFactory.create("ID-5", "OPK-3", false, "Old birch", "Plantae", "Betulaceae")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    List<Map<String, String>> legacyRows =
        legacy.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);
    List<Map<String, String>> mappedRows =
        mapped.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);

    List<Map<String, String>> sortedLegacy =
        legacyRows.stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();
    List<Map<String, String>> sortedMapped =
        mappedRows.stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();

    String scientificName = DwcTerm.scientificName.qualifiedName();
    String kingdom = DwcTerm.kingdom.qualifiedName();
    String family = DwcTerm.family.qualifiedName();

    assertEquals("Occurrence oak", sortedLegacy.get(0).get(scientificName));
    assertEquals("Occurrence oak", sortedMapped.get(0).get(scientificName));
    assertEquals("Plantae", sortedLegacy.get(0).get(kingdom));
    assertEquals("Plantae", sortedMapped.get(0).get(kingdom));
    assertEquals("Fagaceae", sortedLegacy.get(0).get(family));
    assertEquals("Fagaceae", sortedMapped.get(0).get(family));

    // Two accepted identifications are ambiguous: neither may enrich OCC002.
    assertNull(sortedLegacy.get(1).get(kingdom));
    assertNull(sortedMapped.get(1).get(kingdom));
    assertNull(sortedLegacy.get(1).get(family));
    assertNull(sortedMapped.get(1).get(family));

    // No accepted identification likewise contributes nothing to OCC003.
    assertNull(sortedLegacy.get(2).get(kingdom));
    assertNull(sortedMapped.get(2).get(kingdom));
    assertNull(sortedLegacy.get(2).get(family));
    assertNull(sortedMapped.get(2).get(family));
  }

  @Test
  void singleEvidenceMaterialEnrichesOccurrenceIncludingUsagePolicyAndMatchesLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(RowFactory.create("EPK-1", "EVT001")),
            "occurrence",
            occurrencesForMaterial(
                RowFactory.create("OPK-1", "OCC001", "EPK-1", "Occurrence oak"),
                RowFactory.create("OPK-2", "OCC002", "EPK-1", "Occurrence pine")),
            "material",
            materials(
                RowFactory.create(
                    "MEPK-1", "OCC001", "UP-1", "NHM", "CAT-1", "Material oak"),
                RowFactory.create(
                    "MEPK-2", "OCC002", "UP-2", "MZM", "CAT-2A", "Material pine A"),
                RowFactory.create(
                    "MEPK-3", "OCC002", "UP-2", "MZM", "CAT-2B", "Material pine B")),
            "usage-policy",
            usagePolicies(
                RowFactory.create("UP-1", "CC_BY_4_0", "Museum X"),
                RowFactory.create("UP-2", "CC0_1_0", "Museum Y")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    List<Map<String, String>> legacyRows =
        legacy.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);
    List<Map<String, String>> mappedRows =
        mapped.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE);
    List<Map<String, String>> sortedLegacy =
        legacyRows.stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();
    List<Map<String, String>> sortedMapped =
        mappedRows.stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();

    String institutionCode = DwcTerm.institutionCode.qualifiedName();
    String catalogNumber = DwcTerm.catalogNumber.qualifiedName();
    String scientificName = DwcTerm.scientificName.qualifiedName();
    String license = org.gbif.dwc.terms.DcTerm.license.qualifiedName();
    String rightsHolder = org.gbif.dwc.terms.DcTerm.rightsHolder.qualifiedName();

    assertEquals("NHM", sortedLegacy.get(0).get(institutionCode));
    assertEquals("NHM", sortedMapped.get(0).get(institutionCode));
    assertEquals("CAT-1", sortedLegacy.get(0).get(catalogNumber));
    assertEquals("CAT-1", sortedMapped.get(0).get(catalogNumber));
    assertEquals("CC_BY_4_0", sortedLegacy.get(0).get(license));
    assertEquals("CC_BY_4_0", sortedMapped.get(0).get(license));
    assertEquals("Museum X", sortedLegacy.get(0).get(rightsHolder));
    assertEquals("Museum X", sortedMapped.get(0).get(rightsHolder));
    assertEquals("Occurrence oak", sortedLegacy.get(0).get(scientificName));
    assertEquals("Occurrence oak", sortedMapped.get(0).get(scientificName));

    // Two evidence materials make OCC002 ambiguous; none of their fields or usage policy may leak.
    assertNull(sortedLegacy.get(1).get(institutionCode));
    assertNull(sortedMapped.get(1).get(institutionCode));
    assertNull(sortedLegacy.get(1).get(catalogNumber));
    assertNull(sortedMapped.get(1).get(catalogNumber));
    assertNull(sortedLegacy.get(1).get(license));
    assertNull(sortedMapped.get(1).get(license));
    assertNull(sortedLegacy.get(1).get(rightsHolder));
    assertNull(sortedMapped.get(1).get(rightsHolder));
    assertEquals("Occurrence pine", sortedLegacy.get(1).get(scientificName));
    assertEquals("Occurrence pine", sortedMapped.get(1).get(scientificName));
  }

  @Test
  void materialProvenanceMergesDirectAndJunctionLinksByContributionIdentityAndOrder() {
    TableLoader loader =
        TestTableLoader.of(
            "event",
            events(RowFactory.create("EPK-1", "EVT001")),
            "occurrence",
            occurrencesForMaterial(
                RowFactory.create("OPK-1", "OCC001", "EPK-1", "Occurrence oak"),
                RowFactory.create("OPK-2", "OCC002", "EPK-1", "Occurrence pine")),
            "material",
            materialsWithProvenance(
                RowFactory.create("MEPK-1", "OCC001", null, "PPK-2", "NHM", "CAT-1", "Material oak"),
                RowFactory.create("MEPK-2", "OCC002", null, "PPK-1", "MZM", "CAT-2A", "Material pine A"),
                RowFactory.create("MEPK-3", "OCC002", null, "PPK-2", "MZM", "CAT-2B", "Material pine B")),
            "material-provenance",
            materialProvenanceLinks(
                RowFactory.create("MEPK-1", "PPK-2"),
                RowFactory.create("MEPK-1", "PPK-1"),
                RowFactory.create("MEPK-2", "PPK-1"),
                RowFactory.create("MEPK-3", "PPK-2")),
            "provenance",
            provenances(
                RowFactory.create("PPK-1", "PROV-1", "same-funding", "F-1", "P-1", "Alpha"),
                RowFactory.create("PPK-2", "PROV-2", "same-funding", "F-2", "P-2", "Beta")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    List<Map<String, String>> sortedLegacy =
        legacy.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE).stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();
    List<Map<String, String>> sortedMapped =
        mapped.getExtensions().get(OccurrenceMapping.ROW_TYPE_OCCURRENCE).stream()
            .sorted(Comparator.comparing(row -> row.get(DwcTerm.occurrenceID.qualifiedName())))
            .toList();

    String funding = TargetTerms.resolve("fundingAttribution");
    String fundingId = TargetTerms.resolve("fundingAttributionID");
    String projectId = TargetTerms.resolve("projectID");
    String projectTitle = TargetTerms.resolve("projectTitle");

    // PPK-2 is linked both directly and through the junction and must be counted once. PPK-1 is a
    // distinct provenance record with the same funding text, so that duplicate text must survive.
    assertEquals("same-funding|same-funding", sortedLegacy.get(0).get(funding));
    assertEquals("same-funding|same-funding", sortedMapped.get(0).get(funding));
    assertEquals("F-1|F-2", sortedLegacy.get(0).get(fundingId));
    assertEquals("F-1|F-2", sortedMapped.get(0).get(fundingId));
    assertEquals("P-1|P-2", sortedLegacy.get(0).get(projectId));
    assertEquals("P-1|P-2", sortedMapped.get(0).get(projectId));
    assertEquals("Alpha|Beta", sortedLegacy.get(0).get(projectTitle));
    assertEquals("Alpha|Beta", sortedMapped.get(0).get(projectTitle));

    // Two evidence materials make OCC002 ambiguous, so neither material's provenance may leak.
    assertNull(sortedLegacy.get(1).get(funding));
    assertNull(sortedMapped.get(1).get(funding));
    assertNull(sortedLegacy.get(1).get(projectTitle));
    assertNull(sortedMapped.get(1).get(projectTitle));
  }

  @Test
  void materialProvenanceTargetsAreExplicitContributionAwareMerges() {
    CompiledMapping compiled = engine.compile(plan);
    CompiledExtension extension =
        compiled.extensions().stream()
            .filter(e -> e.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .findFirst()
            .orElseThrow();

    for (String column :
        List.of("fundingAttribution", "fundingAttributionID", "projectID", "projectTitle")) {
      String target = TargetTerms.resolve(column);
      var merge =
          extension.targetMerges().stream()
              .filter(candidate -> candidate.targetTerm().equals(target))
              .findFirst()
              .orElseThrow();
      assertEquals(2, merge.producers().size());
      assertTrue(merge.producers().stream().allMatch(p -> p.contributionIdentity().isPresent()));
      assertTrue(merge.producers().stream().allMatch(p -> p.orderBy().isPresent()));
      assertTrue(
          merge.producers().stream()
              .map(p -> p.owner())
              .collect(java.util.stream.Collectors.toSet())
              .containsAll(
                  Set.of(
                      "occurrence-material-direct-provenance",
                      "occurrence-material-provenance")));
    }
  }

  @Test
  void acceptedIdentificationExplicitlyPrecedesMaterialForSharedNonOccurrenceTargets() {
    CompiledMapping compiled = engine.compile(plan);

    for (String column :
        List.of("typeStatus", "geoName", "typeDesignationType", "geoClassificationCode")) {
      String target = TargetTerms.resolve(column);
      MappingDecision decision =
          compiled.decisions().stream()
              .filter(d -> d.targetTerm().equals(target))
              .filter(
                  d ->
                      d.candidates().stream()
                          .anyMatch(c -> c.owner().equals("occurrence-material")))
              .findFirst()
              .orElseThrow();

      assertEquals(MappingDecisionType.EXPLICIT_OVERRIDE, decision.type());
      assertEquals(
          "occurrence-accepted-identification", decision.selected().orElseThrow().owner());
    }
  }

  @Test
  void materialFragmentUsesSchemaBackedWeakRelationRowMatchAndExactlyOne() {
    CompiledMapping compiled = engine.compile(plan);
    CompiledFragment material =
        compiled.extensions().stream()
            .filter(e -> e.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .flatMap(e -> e.fragments().stream())
            .filter(f -> f.name().equals("occurrence-material"))
            .findFirst()
            .orElseThrow();

    assertEquals("occurrence.event_fk", material.scopeKey().qualifiedName());
    assertTrue(material.rowIdentity().isEmpty());
    assertEquals("occurrence.occurrence_pk", material.rowMatch().orElseThrow().qualifiedName());
    assertEquals("usage-policy", material.path().currentResource());
    assertEquals(2, material.path().relations().size());
    assertTrue(material.path().relations().get(0).weak());
    assertEquals("evidenceForOccurrenceID", material.path().relations().get(0).targetColumn());
  }

  @Test
  void acceptedIdentificationFragmentUsesOccurrenceRowMatchAndPreFilterExactlyOne() {
    CompiledMapping compiled = engine.compile(plan);
    CompiledFragment identification =
        compiled.extensions().stream()
            .filter(e -> e.rowType().equals(OccurrenceMapping.ROW_TYPE_OCCURRENCE))
            .flatMap(e -> e.fragments().stream())
            .filter(f -> f.name().equals("occurrence-accepted-identification"))
            .findFirst()
            .orElseThrow();

    assertEquals("occurrence.event_fk", identification.scopeKey().qualifiedName());
    assertTrue(identification.rowIdentity().isEmpty());
    assertEquals(
        "occurrence.occurrence_pk", identification.rowMatch().orElseThrow().qualifiedName());
    assertEquals("identification", identification.path().currentResource());
  }

  private Dataset<Row> events(Row... rows) {
    StructType schema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> occurrences(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> occurrencesWithOrganism(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> occurrencesForIdentification(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> occurrencesForMaterial(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materials(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materialsWithProvenance(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materialProvenanceLinks(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> provenances(Row... rows) {
    StructType schema =
        new StructType()
            .add("provenance_pk", DataTypes.StringType)
            .add("provenanceID", DataTypes.StringType)
            .add("fundingAttribution", DataTypes.StringType)
            .add("fundingAttributionID", DataTypes.StringType)
            .add("projectID", DataTypes.StringType)
            .add("projectTitle", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> usagePolicies(Row... rows) {
    StructType schema =
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> identifications(Row... rows) {
    StructType schema =
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("isAcceptedIdentification", DataTypes.BooleanType)
            .add("scientificName", DataTypes.StringType)
            .add("kingdom", DataTypes.StringType)
            .add("family", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> organisms(Row... rows) {
    StructType schema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }

  private static ExtendedRecord byId(List<ExtendedRecord> records, String id) {
    return records.stream().filter(record -> id.equals(record.getId())).findFirst().orElseThrow();
  }
}
